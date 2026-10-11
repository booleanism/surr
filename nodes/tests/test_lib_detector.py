"""``lib.detector`` and the counting worker that consumes it.

The detector sits behind a narrow interface so that replacing it is a class
plus one line in ``bustle.py``, not an edit to the counting code. These tests
pin that contract: what ``LibreYOLODetector`` does with a library result, and
what ``frame_worker`` does with a ``Detector``.

``libreyolo`` is faked rather than loaded. Loading the real one downloads
79 MB of weights and runs CPU inference at roughly half a second a frame, and
neither belongs in a suite that runs in under a second; the interface is what
is under test here, not the library.
"""

from __future__ import annotations

import asyncio
import dataclasses
import importlib
import sys
import types
import typing
import unittest
import unittest.mock

import numpy

from lib import detector

# ``frame_worker`` is typed against ``core.pipeline.SRTStreamer``, and
# ``core.pipeline`` imports GObject-Introspection at module scope, so the worker
# tests are skipped rather than failed on a host without GStreamer. Declaring
# the name up front keeps the module importable either way; the production call
# site in ``lib/bustle/handlers.py`` is what type-checks the real signature.
processing: typing.Any
GST_AVAILABLE = True
try:
    from lib.bustle import processing
except Exception:  # noqa: BLE001 - any import failure means GStreamer is absent
    GST_AVAILABLE = False

class FakeTensor:
    """Just enough of a torch tensor: ``cpu().numpy()`` and ``astype``."""

    def __init__(self, values: typing.Any) -> None:
        self._values = values

    def cpu(self) -> FakeTensor:
        return self

    def numpy(self) -> numpy.ndarray:
        return numpy.array(self._values)

class FakeBoxes:
    def __init__(self, cls: typing.Any, conf: typing.Any, xyxy: typing.Any) -> None:
        self.cls = FakeTensor(cls)
        self.conf = FakeTensor(conf)
        self.xyxy = FakeTensor(xyxy)

    def __len__(self) -> int:
        return len(self.cls._values)

class FakeResults:
    def __init__(self, boxes: FakeBoxes | None) -> None:
        self.boxes = boxes

class FakeModel:
    """Stands in for what ``libreyolo.LibreYOLO`` returns."""

    def __init__(self, result: FakeResults) -> None:
        self.names = {0: "person", 1: "bicycle", 2: "car"}
        self.result = result
        self.calls: list[tuple[numpy.ndarray, dict[str, typing.Any]]] = []

    def __call__(self, frame: numpy.ndarray, **kwargs: typing.Any) -> FakeResults:
        self.calls.append((frame, kwargs))
        return self.result

def fake_libreyolo(model: FakeModel, seen: list[str]) -> types.ModuleType:
    """A ``libreyolo`` module whose ``LibreYOLO`` records the weights it got."""
    module = types.ModuleType("libreyolo")

    def libre_yolo(weights: str) -> FakeModel:
        seen.append(weights)
        return model

    module.LibreYOLO = libre_yolo  # type: ignore[attr-defined]
    return module

def build_detector(result: FakeResults, **kwargs: typing.Any) -> tuple[detector.LibreYOLODetector, FakeModel, list[str]]:
    """Construct a detector against a faked ``libreyolo``.

    The patch only has to cover construction: the constructor is where the
    import happens, and afterwards the object holds the fake model.
    """
    model = FakeModel(result)
    seen: list[str] = []
    with unittest.mock.patch.dict(sys.modules, {"libreyolo": fake_libreyolo(model, seen)}):
        det = detector.LibreYOLODetector(**kwargs)
    return det, model, seen

FRAME = numpy.zeros((4, 4, 3), dtype=numpy.uint8)

class LibreYOLODetectorTests(unittest.TestCase):
    def test_importing_the_module_does_not_require_the_detector(self):
        # The counting code and these tests both import lib.detector; if that
        # pulled in libreyolo, the interface would be unusable anywhere the
        # detector is not installed. The import is deliberately in the
        # constructor instead.
        saved = sys.modules.pop("libreyolo", None)
        try:
            importlib.reload(detector)
            self.assertNotIn("libreyolo", sys.modules)
        finally:
            if saved is not None:
                sys.modules["libreyolo"] = saved

    def test_the_shipped_weights_are_the_permissive_ones(self):
        # DEIM version 1 at size m: Apache-2.0 weights, no DINOv3 backbone. A
        # bump to DEIMv2 S/M/L/X would put Meta's non-OSI licence on the
        # shipped weights.
        self.assertEqual(detector.DEFAULT_WEIGHTS, "LibreDEIMm.pt")

    def test_default_weights_reach_the_library(self):
        _, _, seen = build_detector(FakeResults(None))
        self.assertEqual(seen, [detector.DEFAULT_WEIGHTS])

    def test_a_given_weights_path_is_passed_through(self):
        _, _, seen = build_detector(FakeResults(None), weights="/mnt/models/other.pt")
        self.assertEqual(seen, ["/mnt/models/other.pt"])

    def test_names_proxies_the_model(self):
        det, model, _ = build_detector(FakeResults(None))
        self.assertEqual(det.names, model.names)
        self.assertEqual(det.names[0], "person")

    def test_detections_are_plain_python(self):
        boxes = FakeBoxes(cls=[0, 2], conf=[0.9, 0.4], xyxy=[[1, 2, 3, 4], [5, 6, 7, 8]])
        det, _, _ = build_detector(FakeResults(boxes))

        got = det.detect(FRAME)

        self.assertEqual(
            got,
            [
                detector.Detection(0, 0.9, (1.0, 2.0, 3.0, 4.0)),
                detector.Detection(2, 0.4, (5.0, 6.0, 7.0, 8.0)),
            ],
        )
        self.assertIsInstance(got[0].class_id, int)
        self.assertIsInstance(got[0].confidence, float)
        self.assertIsInstance(got[0].xyxy[0], float)

    def test_the_thresholds_reach_the_model(self):
        det, model, _ = build_detector(FakeResults(None), confidence=0.6, max_detections=7)

        det.detect(FRAME)

        self.assertEqual(model.calls, [(FRAME, {"conf": 0.6, "max_det": 7})])

    def test_the_library_defaults_are_used_when_nothing_is_given(self):
        det, model, _ = build_detector(FakeResults(None))

        det.detect(FRAME)

        self.assertEqual(
            model.calls,
            [(FRAME, {"conf": detector.DEFAULT_CONFIDENCE, "max_det": detector.DEFAULT_MAX_DETECTIONS})],
        )

    def test_no_boxes_is_an_empty_list(self):
        det, _, _ = build_detector(FakeResults(None))
        self.assertEqual(det.detect(FRAME), [])

    def test_an_empty_batch_is_an_empty_list(self):
        det, _, _ = build_detector(FakeResults(FakeBoxes(cls=[], conf=[], xyxy=[])))
        self.assertEqual(det.detect(FRAME), [])

    def test_a_detection_is_frozen(self):
        one = detector.Detection(1, 0.5, (0.0, 0.0, 1.0, 1.0))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            one.class_id = 2  # type: ignore[misc]

class FakeReceiver:
    """Feeds scripted frames and then flips ``running`` off."""

    def __init__(self, frames: list[numpy.ndarray]) -> None:
        self._frames = list(frames)
        self.running = True
        self.waits: list[float | None] = []

    def get_frame(self, timeout: float | None = None) -> numpy.ndarray | None:
        self.waits.append(timeout)
        if not self._frames:
            self.running = False
            return None
        return self._frames.pop(0)

class FakeDetector:
    """Returns one scripted batch per call; an Exception in the script raises."""

    def __init__(self, names: dict[int, str], script: list[typing.Any]) -> None:
        self._names = names
        self._script = list(script)
        self.seen: list[numpy.ndarray] = []

    @property
    def names(self) -> dict[int, str]:
        return self._names

    def detect(self, frame: numpy.ndarray) -> list[detector.Detection]:
        self.seen.append(frame)
        outcome = self._script.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

def box(class_id: int, confidence: float = 0.9) -> detector.Detection:
    return detector.Detection(class_id, confidence, (0.0, 0.0, 1.0, 1.0))

@unittest.skipUnless(GST_AVAILABLE, "needs GStreamer (core.pipeline imports gi)")
class FrameWorkerTests(unittest.IsolatedAsyncioTestCase):
    NAMES = {0: "person", 1: "bicycle", 2: "car", 3: "motorcycle"}
    CHOSEN = [0, 1, 2, 3]

    async def _drain(self, receiver: FakeReceiver, det: FakeDetector) -> list[dict[str, int]]:
        loop = asyncio.get_running_loop()
        q: asyncio.Queue[dict[str, int]] = asyncio.Queue()

        await asyncio.to_thread(
            processing.frame_worker,
            receiver,
            det,
            self.CHOSEN,
            loop,
            q,
        )
        # call_soon_threadsafe schedules the puts; give the loop a turn to run
        # them before reading the queue.
        for _ in range(3):
            await asyncio.sleep(0)

        out: list[dict[str, int]] = []
        while not q.empty():
            out.append(q.get_nowait())
        return out

    async def test_every_chosen_class_is_reported_even_at_zero(self):
        det = FakeDetector(self.NAMES, [[box(0), box(0), box(2)]])

        got = await self._drain(FakeReceiver([FRAME]), det)

        self.assertEqual(got, [{"person": 2, "bicycle": 0, "car": 1, "motorcycle": 0}])

    async def test_a_class_outside_the_name_map_is_not_counted(self):
        det = FakeDetector(self.NAMES, [[box(0), box(79)]])

        got = await self._drain(FakeReceiver([FRAME]), det)

        self.assertEqual(got, [{"person": 1, "bicycle": 0, "car": 0, "motorcycle": 0}])

    async def test_the_frame_reaches_the_detector_unchanged(self):
        frame = numpy.arange(12, dtype=numpy.uint8).reshape(2, 2, 3)
        det = FakeDetector(self.NAMES, [[]])

        await self._drain(FakeReceiver([frame]), det)

        self.assertEqual(len(det.seen), 1)
        self.assertIs(det.seen[0], frame)

    async def test_a_failing_frame_does_not_stop_the_worker(self):
        frames = [FRAME, FRAME, FRAME]
        det = FakeDetector(self.NAMES, [RuntimeError("cuda exploded"), [], [box(1)]])

        got = await self._drain(FakeReceiver(frames), det)

        self.assertEqual(len(det.seen), 3)
        self.assertEqual(
            got,
            [
                {"person": 0, "bicycle": 0, "car": 0, "motorcycle": 0},
                {"person": 0, "bicycle": 1, "car": 0, "motorcycle": 0},
            ],
        )

    async def test_an_empty_frame_list_does_not_block(self):
        det = FakeDetector(self.NAMES, [])

        got = await self._drain(FakeReceiver([]), det)

        self.assertEqual(got, [])
        self.assertEqual(det.seen, [])

    async def test_it_waits_with_the_frame_timeout(self):
        receiver = FakeReceiver([FRAME])

        await self._drain(receiver, FakeDetector(self.NAMES, [[]]))

        self.assertEqual(receiver.waits, [processing.FRAME_WAIT_SECONDS, processing.FRAME_WAIT_SECONDS])
