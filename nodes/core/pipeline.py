import queue
import threading
import typing
import uuid
import numpy

import gi
gi.require_version('Gst', '1.0')
gi.require_version('GstApp', '1.0')
gi.require_version('GstVideo', '1.0')

from gi.repository import Gst, GstVideo

class SRTStreamer:
    def __init__(self, node_id: uuid.UUID):
        self.node_id = node_id
        self.pipeline = None
        self.running = False
        self._frame_q: queue.Queue[numpy.ndarray] = queue.Queue(maxsize=1)
        # Guards pipeline/running. The GStreamer streaming thread runs
        # _on_new_sample concurrently with start()/stop() on the event loop, and
        # without this the callback could touch a pipeline that stop() already
        # set to NULL.
        self._lock = threading.Lock()

    def _on_new_sample(self, sink):
        # Held for the whole callback so stop() cannot set the pipeline to NULL
        # underneath a half-finished pull. stop() itself calls set_state(NULL)
        # only after releasing this lock, which is what keeps that safe: the
        # streaming thread never waits on set_state, so the two cannot deadlock.
        with self._lock:
            if not self.running:
                return Gst.FlowReturn.OK

            sample = sink.emit("pull-sample")
            if not sample:
                return Gst.FlowReturn.ERROR

            buffer = sample.get_buffer()
            caps = sample.get_caps()
            struct = caps.get_structure(0)

            width = struct.get_value("width")
            height = struct.get_value("height")

            success, map_info = buffer.map(Gst.MapFlags.READ)
            if not success:
                return Gst.FlowReturn.ERROR

            try:
                frame = self._frame_view(map_info, caps, width, height)
            finally:
                # Safe even though `frame` points at this data: reading
                # MapInfo.data produced a Python bytes object that owns its own
                # memory, so it outlives the unmap (see _frame_view).
                buffer.unmap(map_info)

            if frame is None:
                return Gst.FlowReturn.ERROR

        try:
            self._frame_q.put_nowait(frame)
        except queue.Full:
            # Drop the stale frame. Both calls can still fail: the consumer may
            # drain the queue between put_nowait and get_nowait, and may fill it
            # again before the second put. Raising here would abort the
            # GStreamer streaming thread.
            try:
                self._frame_q.get_nowait()
            except queue.Empty:
                pass

            try:
                self._frame_q.put_nowait(frame)
            except queue.Full:
                pass

        return Gst.FlowReturn.OK

    @staticmethod
    def _frame_view(map_info, caps, width: int, height: int):
        """Return a numpy view over the mapped frame, with no extra copy.

        PyGObject's MapInfo.data is a Python `bytes`, not a memoryview: the
        attribute access itself performs one memcpy out of the buffer, and the
        result owns its memory and outlives `unmap`. numpy.frombuffer only
        wraps that object, so the `.copy()` this used to do was a second full
        frame memcpy (~420us at 1280x720) on the GStreamer streaming thread,
        protecting nothing.

        The array is read-only, being backed by an immutable bytes.
        ultralytics' LetterBox accepts that: it resizes into a fresh array.

        Rows are strided rather than reshaped. videoconvert pads each row to an
        alignment, so the stride is not `width * 3` whenever a row is not
        4-byte aligned (1279 wide -> stride 3840, not 3837), and reshaping
        would skew the image by the accumulated padding.

        Returns None when the mapped buffer is too small for the layout the
        caps describe, so the caller can fail the sample instead of reading
        past the end.
        """
        img_data = numpy.frombuffer(map_info.data, dtype=numpy.uint8)

        info = GstVideo.VideoInfo.new_from_caps(caps)
        if info is None:
            # new_from_caps returns None when the caps do not describe a video
            # format. The appsink caps always do, but packed rows are the only
            # assumption available if it ever does not.
            stride, offset = width * 3, 0
        else:
            stride = info.stride[0] if info.stride[0] > 0 else width * 3
            offset = info.offset[0]

        if offset + stride * (height - 1) + width * 3 > img_data.size:
            # The caps-derived layout does not fit the mapped buffer, so the
            # producer used one we cannot see (a GstVideoMeta override, which
            # PyGObject does not expose). Tightly packed rows are the only
            # assumption left that cannot read past the end.
            stride, offset = width * 3, 0
        if offset + stride * (height - 1) + width * 3 > img_data.size:
            return None

        return numpy.ndarray(
            (height, width, 3),
            dtype=numpy.uint8,
            buffer=img_data,
            offset=offset,
            strides=(stride, 3, 1),
        )

    def start(self, srt_address: str):
        pipeline_str = (
            'srtsrc name=src poll-timeout=10000 ! '
            'tsdemux ! h264parse ! avdec_h264 ! videoconvert ! '
            'video/x-raw, format=BGR ! appsink name=app_sink emit-signals=true max-buffers=1 drop=true'
        )

        pipeline = typing.cast(Gst.Pipeline, Gst.parse_launch(pipeline_str))
        if not pipeline:
            raise ValueError("unable to build pipeline")

        src = pipeline.get_by_name("src")
        if not src:
            raise ValueError("expected Element")

        uri = f"srt://{srt_address}?mode=caller&streamid=subscribe:{self.node_id}"
        src.set_property("uri", uri)

        appsink = pipeline.get_by_name("app_sink")
        if appsink is None:
            raise TypeError("appsink is none")
        appsink.connect("new-sample", self._on_new_sample)

        # Publish the new pipeline only once it is fully wired, so a failed
        # start() cannot leave a half-built pipeline visible to stop().
        with self._lock:
            self.running = True
            self.pipeline = pipeline

        pipeline.set_state(Gst.State.PLAYING)

    def get_frame(self, timeout: typing.Optional[float] = None):
        """Return the next frame, or None if none is available.

        timeout=None is non-blocking, which is what the old get_nowait() did.
        A positive timeout blocks the calling thread for at most that long,
        which is what keeps frame_worker from spinning at 100% CPU.

        The array is a read-only view over the sample's own memory, so a caller
        that needs to mutate it must copy first. It is deliberately not copied
        here: see _frame_view.
        """
        try:
            if timeout is None:
                return self._frame_q.get_nowait()
            return self._frame_q.get(timeout=timeout)
        except queue.Empty:
            return None

    def stop(self):
        with self._lock:
            self.running = False
            pipeline = self.pipeline
            self.pipeline = None

        # Outside the lock: set_state(NULL) blocks until the streaming thread
        # stops, and that thread takes this same lock in _on_new_sample.
        if pipeline is not None:
            pipeline.set_state(Gst.State.NULL)
