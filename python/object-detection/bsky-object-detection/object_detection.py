import logging
import os
import time
from typing import List, Tuple

import torch
from opentelemetry import trace
from PIL import Image
from transformers import DetrForObjectDetection, DetrImageProcessor

from .models import DetectionResult, ImageMeta

logger = logging.getLogger(__name__)


MODEL = f"facebook/detr-resnet-50"

device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

processor = DetrImageProcessor.from_pretrained(MODEL)
model = DetrForObjectDetection.from_pretrained(MODEL).to(device)
model.save_pretrained(f"{os.getcwd()}/{MODEL}")


def detect_objects(
    image_pairs: List[Tuple[ImageMeta, Image.Image]]
) -> List[Tuple[ImageMeta, List[DetectionResult]]]:
    tracer = trace.get_tracer("bsky-object-detection")
    with tracer.start_as_current_span("detect_objects") as span:
        start = time.time()

        images = [img for _, img in image_pairs]

        inputs = processor(images=images, return_tensors="pt").to(device)
        with tracer.start_as_current_span("model_inference"):
            outputs = model(**inputs)

        # convert outputs (bounding boxes and class logits) to COCO API
        # let's only keep detections with score > 0.5
        target_sizes = torch.tensor([img.size[::-1] for img in images])
        model_results = processor.post_process_object_detection(
            outputs, target_sizes=target_sizes, threshold=0.5
        )

        processing_time = time.time() - start
        span.set_attribute("processing_time", processing_time)

        batch_detection_results: List[Tuple[ImageMeta, List[DetectionResult]]] = []

        for idx, result in enumerate(model_results):
            detection_results: List[DetectionResult] = []
            logger.debug(f"Detection results: {result}")

            for score, label, box in zip(
                result["scores"], result["labels"], result["boxes"]
            ):
                box = [round(i, 2) for i in box.tolist()]
                detection_results.append(
                    DetectionResult(
                        label=model.config.id2label[label.item()],
                        confidence=round(score.item(), 5),
                        box=box,
                    )
                )

            batch_detection_results.append((image_pairs[idx][0], detection_results))

        return batch_detection_results
