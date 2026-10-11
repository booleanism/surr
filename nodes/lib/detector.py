"""Object detection, behind a narrow interface.

Counting needs two things from a detector: the class-id to name mapping, and a
per-frame list of detections. Everything else -- which library, which
checkpoint, whether it is a YOLO or a DETR -- is an implementation detail, so
`frame_worker` talks to `Detector` and never to a model object.

That indirection is the point. The counting code used to call `model.track()`
and read `boxes.cls`, `isinstance(boxes.cls, torch.Tensor)` and `model.names`
straight out of an ultralytics `Results`, which is why replacing the detector
meant editing the counting code. `LibreYOLODetector` is the implementation the
node ships with; a different one is a class here plus one line in `bustle.py`.
"""

import dataclasses
import typing

import numpy

# The detector the node ships with. LibreYOLO's DEIM family is a DETR-style
# dense one-to-one detector whose weights are Apache-2.0, which is what let the
# node drop ultralytics (AGPL-3.0) without this repository's MIT licence
# becoming a misstatement. `m` is the middle size: 55.4 mAP50-95 on COCO
# val2017 at 19.6M parameters, 640 px input.
DEFAULT_WEIGHTS = "LibreDEIMm.pt"

# Detections below this score are dropped. A DETR-style decoder emits a fixed
# number of queries and applies no NMS, so this threshold is the only thing
# between the raw output and the count: at the library's own default (0.25) one
# object can produce several low-confidence boxes and every one of them is
# counted. Raise it when the counts of crowded classes read high.
DEFAULT_CONFIDENCE = 0.25
DEFAULT_MAX_DETECTIONS = 300


@dataclasses.dataclass(frozen=True)
class Detection:
    """One box, in pixel coordinates of the frame it was detected in."""

    class_id: int
    confidence: float
    xyxy: tuple[float, float, float, float]


class Detector(typing.Protocol):
    """What the frame worker needs from a detector."""

    @property
    def names(self) -> typing.Mapping[int, str]: ...

    def detect(self, frame: numpy.ndarray) -> list[Detection]: ...


class LibreYOLODetector:
    """`Detector` backed by LibreYOLO (https://libreyolo.com)."""

    def __init__(
        self,
        weights: str = DEFAULT_WEIGHTS,
        confidence: float = DEFAULT_CONFIDENCE,
        max_detections: int = DEFAULT_MAX_DETECTIONS,
    ) -> None:
        # Imported here rather than at module scope: the interface, the
        # counting code and the tests all import this module, and none of them
        # should need the detector installed to do it.
        from libreyolo import LibreYOLO

        # A bare filename resolves (and downloads on first use) under
        # ./weights; a path with a directory component is taken literally and
        # never fetched.
        self._model = LibreYOLO(weights)
        self._confidence = confidence
        self._max_detections = max_detections

    @property
    def names(self) -> typing.Mapping[int, str]:
        return self._model.names

    def detect(self, frame: numpy.ndarray) -> list[Detection]:
        # A single image returns one Results, not a list: predict() only
        # returns a sequence for a list of sources, or with stream=True.
        #
        # The frame is handed over as it arrives from the pipeline -- a
        # read-only strided view over the GstBuffer -- and is not copied here.
        result = self._model(
            frame, conf=self._confidence, max_det=self._max_detections
        )

        boxes = result.boxes
        if boxes is None:
            return []

        classes = boxes.cls.cpu().numpy().astype(int)
        confidences = boxes.conf.cpu().numpy()
        coordinates = boxes.xyxy.cpu().numpy()
        return [
            Detection(
                class_id=int(class_id),
                confidence=float(confidence),
                xyxy=(float(x1), float(y1), float(x2), float(y2)),
            )
            for class_id, confidence, (x1, y1, x2, y2) in zip(
                classes, confidences, coordinates
            )
        ]
