import threading
import queue
import typing
import uuid
import numpy

import gi
gi.require_version('Gst', '1.0')
gi.require_version('GstApp', '1.0')

from gi.repository import Gst

class SRTStreamer:
    def __init__(self, node_id: uuid.UUID):
        self.node_id = node_id
        self.pipeline = None
        self.running = False
        self._frame_q: queue.Queue = queue.Queue(maxsize=1)

    def _on_new_sample(self, sink):
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

        img_data = numpy.frombuffer(map_info.data, dtype=numpy.uint8).reshape((height, width, 3))
        frame_copy = img_data.copy()

        buffer.unmap(map_info)

        try:
            self._frame_q.put_nowait(frame_copy)
        except queue.Full:
            self._frame_q.get_nowait()   # drop stale frame
            self._frame_q.put_nowait(frame_copy)
        return Gst.FlowReturn.OK

    def start(self, srt_address: str):
        pipeline_str = (
            'srtsrc name=src poll-timeout=10000 ! '
            'tsdemux ! h264parse ! avdec_h264 ! videoconvert ! '
            'video/x-raw, format=BGR ! appsink name=app_sink emit-signals=true max-buffers=1 drop=true'
        )
        self.pipeline = typing.cast(Gst.Pipeline, Gst.parse_launch(pipeline_str))

        src = self.pipeline.get_by_name("src")
        if not src:
            raise ValueError("expected Element")

        uri = f"srt://{srt_address}?mode=caller&streamid=subscribe:{self.node_id}"
        src.set_property("uri", uri)

        appsink = self.pipeline.get_by_name("app_sink")
        if appsink is None:
            raise TypeError("appsink is none")
        appsink.connect("new-sample", self._on_new_sample)

        self.running = True
        self.pipeline.set_state(Gst.State.PLAYING)

    def get_frame(self):
        try:
            return self._frame_q.get_nowait()
        except queue.Empty:
            return None

    def stop(self):
        self.running = False
        if self.pipeline:
            self.pipeline.set_state(Gst.State.NULL)
            self.pipeline = None
