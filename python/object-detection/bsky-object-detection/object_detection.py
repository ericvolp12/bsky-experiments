import logging
import os
import time
from typing import List

import torch
from opentelemetry import trace
from PIL import Image
from transformers import DetrForObjectDetection, DetrImageProcessor

from .models import DetectionResult

logger = logging.getLogger(__name__)


MODEL = f"facebook/detr-resnet-50"

device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

processor = DetrImageProcessor.from_pretrained(MODEL)
model = DetrForObjectDetection.from_pretrained(MODEL).to(device)
model.save_pretrained(f"{os.getcwd()}/{MODEL}")


def detect_objects(image: Image.Image) -> List[DetectionResult]:
    tracer = trace.get_tracer("bsky-object-detection")
    with tracer.start_as_current_span("detect_objects") as span:
        start = time.time()

        inputs = processor(images=[image], return_tensors="pt").to(device)
        with tracer.start_as_current_span("model_inference"):
            outputs = model(**inputs)

        # convert outputs (bounding boxes and class logits) to COCO API
        # let's only keep detections with score > 0.75
        target_sizes = torch.tensor([image.size[::-1]])
        model_results = processor.post_process_object_detection(
            outputs, target_sizes=target_sizes, threshold=0.75
        )[0]

        processing_time = time.time() - start
        # If it takes longer than 1 second to process, add some attributes to the span
        if processing_time > 1:
            span.set_attribute("slow_process", True)
            span.set_attribute("processing_time", processing_time)
            span.set_attribute("post_text", image)

        detection_results: List[DetectionResult] = []

        logger.info(f"Detection results: {model_results}")

        for score, label, box in zip(
            model_results["scores"], model_results["labels"], model_results["boxes"]
        ):
            box = [round(i, 2) for i in box.tolist()]
            detection_results.append(
                DetectionResult(
                    label=model.config.id2label[label.item()],
                    confidence=round(score.item(), 5),
                    box=box,
                )
            )

        return detection_results
