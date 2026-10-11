"""``lib.core.pipeline``: the SRT frame path.

The regression these lock down: ``videoconvert`` pads every row to an
alignment, so the stride is not ``width * 3`` whenever a row is not 4-byte
aligned (1279 wide -> stride 3840, not 3837). The frame used to be built with
``reshape((height, width, 3))``, which raised ``ValueError`` on the GStreamer
streaming thread for those widths instead of producing a skewed image -- it
went unnoticed only because ``--downstream`` defaults to a 1280-wide frame.

They also pin the deliberate absence of a copy: the returned array is a
read-only view over the mapped sample's own memory, which is why it stays
readable after ``buffer.unmap``.
"""

from __future__ import annotations

import threading
import typing
import unittest
import unittest.mock
import uuid

import numpy

# The GStreamer bindings are a compiled extension that may simply not be
# installed, so the names below are bound only when the probe succeeds; every
# test that touches them is skipped otherwise (see GST_AVAILABLE). Declaring
# them up front keeps the module importable, and type checkable, either way.
Gst: typing.Any
GstVideo: typing.Any
SRTStreamer: typing.Any

try:  # pragma: no cover - the guard is exercised only where GStreamer is absent
    import gi

    gi.require_version("Gst", "1.0")
    gi.require_version("GstVideo", "1.0")

    from gi.repository import Gst, GstVideo

    Gst.init(None)

    from lib.core.pipeline import SRTStreamer

    GST_AVAILABLE = True
    GST_ERROR = ""
except Exception as exc:  # pragma: no cover - depends on the host
    GST_AVAILABLE = False
    GST_ERROR = f"{type(exc).__name__}: {exc}"

# Above every value the test pattern can produce (it stays in 0..250), so a
# byte of row padding is unambiguous.
POISON = 255

def pattern(width: int, height: int) -> numpy.ndarray:
    """A (height, width, 3) BGR image whose three channels differ."""
    ys = numpy.arange(height).reshape(height, 1)
    xs = numpy.arange(width).reshape(1, width)
    red = (ys + xs) % 251
    green = (2 * ys) % 251
    blue = (3 * xs) % 251
    return numpy.stack(numpy.broadcast_arrays(red, green, blue), axis=-1).astype(numpy.uint8)

def caps_for(width: int, height: int):
    return Gst.Caps.from_string(f"video/x-raw,format=BGR,width={width},height={height}")

def stride_for(width: int, height: int) -> int:
    return GstVideo.VideoInfo.new_from_caps(caps_for(width, height)).stride[0]

def padded_buffer(width: int, height: int) -> tuple[bytes, int]:
    """Serialise the pattern with the stride the caps imply, padding with POISON."""
    stride = stride_for(width, height)
    rows = numpy.full((height, stride), POISON, dtype=numpy.uint8)
    rows[:, : width * 3] = pattern(width, height).reshape(height, width * 3)
    return rows.tobytes(), stride

class FakeSample:
    def __init__(self, buffer, caps) -> None:
        self._buffer = buffer
        self._caps = caps

    def get_buffer(self):
        return self._buffer

    def get_caps(self):
        return self._caps

class FakeSink:
    def __init__(self, sample) -> None:
        self._sample = sample

    def emit(self, name: str):
        assert name == "pull-sample"
        return self._sample

@unittest.skipUnless(GST_AVAILABLE, f"GStreamer bindings unavailable ({GST_ERROR})")
class FrameViewTests(unittest.TestCase):
    # Widths whose rows videoconvert pads (measured strides: 641 -> 1924,
    # 999 -> 3000, 1279 -> 3840).
    PADDED_WIDTHS = (641, 999, 1279)
    HEIGHT = 37

    def _view(self, width: int, height: int, data: bytes):
        buffer = Gst.Buffer.new_wrapped(data)
        ok, map_info = buffer.map(Gst.MapFlags.READ)
        self.assertTrue(ok)
        try:
            return SRTStreamer._frame_view(map_info, caps_for(width, height), width, height)
        finally:
            buffer.unmap(map_info)

    def test_padded_widths_really_are_padded(self):
        # Guards the three tests below from silently becoming no-ops.
        for width in self.PADDED_WIDTHS:
            with self.subTest(width=width):
                stride = stride_for(width, self.HEIGHT)
                self.assertGreater(stride, width * 3)

    def test_padded_rows_are_decoded_without_leaking_padding(self):
        for width in self.PADDED_WIDTHS:
            with self.subTest(width=width):
                data, stride = padded_buffer(width, self.HEIGHT)
                frame = self._view(width, self.HEIGHT, data)

                self.assertIsNotNone(frame)
                self.assertEqual(frame.shape, (self.HEIGHT, width, 3))
                numpy.testing.assert_array_equal(frame, pattern(width, self.HEIGHT))
                self.assertFalse(numpy.any(frame == POISON), "row padding leaked into the frame")
                self.assertEqual(frame.strides[0], stride)

    def test_a_tightly_packed_row_is_decoded_unchanged(self):
        # 1280 * 3 is 4-byte aligned, which is why the reshape bug never bit.
        width, height = 1280, 8
        self.assertEqual(stride_for(width, height), width * 3)
        data, _ = padded_buffer(width, height)

        frame = self._view(width, height, data)

        self.assertEqual(frame.shape, (height, width, 3))
        numpy.testing.assert_array_equal(frame, pattern(width, height))

    def test_a_tightly_packed_buffer_falls_back_when_the_caps_overstate_it(self):
        # The caps describe padded rows but the buffer only holds packed ones,
        # so the caps-derived layout does not fit and the packed fallback runs.
        width, height = 1279, 8
        self.assertGreater(stride_for(width, height), width * 3)
        data = pattern(width, height).reshape(-1).tobytes()

        frame = self._view(width, height, data)

        self.assertIsNotNone(frame)
        self.assertEqual(frame.shape, (height, width, 3))
        numpy.testing.assert_array_equal(frame, pattern(width, height))

    def test_caps_that_do_not_describe_video_fall_back_to_packed_rows(self):
        # GstVideo.VideoInfo.new_from_caps returns None for caps that are not a
        # video format; the appsink caps always are, but packed rows are the
        # only layout left to assume when it happens.
        width, height = 1279, 8
        data = pattern(width, height).reshape(-1).tobytes()

        with unittest.mock.patch.object(GstVideo.VideoInfo, "new_from_caps", return_value=None):
            frame = self._view(width, height, data)

        self.assertIsNotNone(frame)
        self.assertEqual(frame.shape, (height, width, 3))
        numpy.testing.assert_array_equal(frame, pattern(width, height))

    def test_a_buffer_too_small_for_the_caps_yields_none(self):
        width, height = 1280, 720
        data = b"\x00" * 1024

        self.assertIsNone(self._view(width, height, data))

    def test_a_one_pixel_frame_is_decoded(self):
        frame = self._view(1, 1, b"\x00\x00\x00")
        self.assertEqual(frame.shape, (1, 1, 3))

    def test_the_frame_is_a_read_only_view_over_the_mapped_buffer(self):
        width, height = 1279, 8
        data, _ = padded_buffer(width, height)
        frame = self._view(width, height, data)

        # A copy would leave frame.base as None; two levels of .base means the
        # array is a view over the frombuffer() array, which is a view over the
        # bytes object PyGObject produced.
        self.assertIsInstance(frame.base, numpy.ndarray)
        self.assertIsInstance(frame.base.base, bytes)
        self.assertFalse(frame.flags.writeable)

        with self.assertRaises(ValueError):
            frame[0, 0, 0] = 1

    def test_the_frame_outlives_the_mapping(self):
        # _on_new_sample unmaps before the frame is queued, which is only safe
        # because MapInfo.data produced a bytes object owning its own memory.
        width, height = 1279, 8
        data, _ = padded_buffer(width, height)
        expected = pattern(width, height)

        buffer = Gst.Buffer.new_wrapped(data)
        ok, map_info = buffer.map(Gst.MapFlags.READ)
        self.assertTrue(ok)
        frame = SRTStreamer._frame_view(map_info, caps_for(width, height), width, height)
        buffer.unmap(map_info)

        numpy.testing.assert_array_equal(frame, expected)

@unittest.skipUnless(GST_AVAILABLE, f"GStreamer bindings unavailable ({GST_ERROR})")
class OnNewSampleTests(unittest.TestCase):
    WIDTH, HEIGHT = 1279, 8

    def setUp(self) -> None:
        self.streamer = SRTStreamer(uuid.uuid4())
        self.data, _ = padded_buffer(self.WIDTH, self.HEIGHT)

    def _sink(self, data: bytes | None = None):
        buffer = Gst.Buffer.new_wrapped(self.data if data is None else data)
        return FakeSink(FakeSample(buffer, caps_for(self.WIDTH, self.HEIGHT)))

    def test_a_sample_is_ignored_while_not_running(self):
        self.assertIs(self.streamer._on_new_sample(self._sink()), Gst.FlowReturn.OK)
        self.assertTrue(self.streamer._frame_q.empty())

    def test_a_sample_is_queued_for_the_worker(self):
        self.streamer.running = True

        self.assertIs(self.streamer._on_new_sample(self._sink()), Gst.FlowReturn.OK)

        frame = self.streamer.get_frame()
        self.assertIsNotNone(frame)
        numpy.testing.assert_array_equal(frame, pattern(self.WIDTH, self.HEIGHT))

    def test_a_buffer_that_does_not_match_the_caps_fails_the_sample(self):
        self.streamer.running = True

        self.assertIs(self.streamer._on_new_sample(self._sink(b"\x00" * 16)), Gst.FlowReturn.ERROR)
        self.assertTrue(self.streamer._frame_q.empty())

    def test_a_full_queue_drops_the_stale_frame_instead_of_raising(self):
        self.streamer.running = True
        second = pattern(self.WIDTH, self.HEIGHT) + 1

        self.streamer._on_new_sample(self._sink())
        self.streamer._on_new_sample(self._sink(second.astype(numpy.uint8).tobytes()))

        # maxsize is 1 and the newest frame is the one the worker should see.
        self.assertEqual(self.streamer._frame_q.qsize(), 1)
        numpy.testing.assert_array_equal(self.streamer.get_frame(), second)

    def test_get_frame_returns_none_when_nothing_arrived(self):
        self.assertIsNone(self.streamer.get_frame(timeout=0.01))

    def test_get_frame_without_a_timeout_does_not_block(self):
        # timeout=None used to mean get_nowait(); a plain Queue.get(timeout=None)
        # blocks forever instead. Called off-thread (and never with an
        # unbounded timeout on this thread) so a regression shows up as a
        # failure rather than a hung suite.
        result: list[object] = []
        thread = threading.Thread(target=lambda: result.append(self.streamer.get_frame()))
        thread.start()
        thread.join(timeout=5)

        self.assertFalse(thread.is_alive(), "get_frame() blocked: timeout=None must be non-blocking")
        self.assertEqual(result, [None])

    def test_get_frame_without_a_timeout_still_returns_a_queued_frame(self):
        self.streamer.running = True
        self.streamer._on_new_sample(self._sink())

        numpy.testing.assert_array_equal(
            self.streamer.get_frame(), pattern(self.WIDTH, self.HEIGHT)
        )

@unittest.skipUnless(GST_AVAILABLE, f"GStreamer bindings unavailable ({GST_ERROR})")
class LifecycleTests(unittest.TestCase):
    def test_stop_without_a_pipeline_is_a_no_op(self):
        streamer = SRTStreamer(uuid.uuid4())
        streamer.running = True

        streamer.stop()

        self.assertFalse(streamer.running)
        self.assertIsNone(streamer.pipeline)

    def test_frames_left_over_from_a_previous_run_are_still_readable(self):
        # stop() swaps the pipeline out but leaves the queue alone, so the
        # worker drains the last frame before the receiver reports stopped.
        streamer = SRTStreamer(uuid.uuid4())
        data, _ = padded_buffer(16, 4)
        buffer = Gst.Buffer.new_wrapped(data)
        streamer.running = True
        streamer._on_new_sample(FakeSink(FakeSample(buffer, caps_for(16, 4))))

        streamer.stop()

        self.assertIsNotNone(streamer.get_frame())

if __name__ == "__main__":
    unittest.main()
