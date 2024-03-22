import logging
import os
import time
from typing import List, Tuple
from unittest import result

import torch
from opentelemetry import trace
from PIL import Image
from transformers import CLIPProcessor, CLIPModel, BatchFeature

from .models import DetectionResult, ImageMeta

logger = logging.getLogger(__name__)


MODEL = f"openai/clip-vit-large-patch14"

device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

processor = CLIPProcessor.from_pretrained(MODEL)
model = CLIPModel.from_pretrained(MODEL).to(device)
model.save_pretrained(f"{os.getcwd()}/{MODEL}")

tracer = trace.get_tracer("bsky-object-detection")

imageClasses = [
    "a picture of a cat",
    "a picture of a dog",
    "a picture of a bird",
    "a picture of food",
    "a picture of a cute animal",
]

labels = [
    "cat",
    "dog",
    "bird",
    "food",
    "animal",
]


def detect_objects(
    batch: BatchFeature, image_pairs: List[Tuple[ImageMeta, Image.Image]]
) -> List[Tuple[ImageMeta, List[DetectionResult]]]:
    with tracer.start_as_current_span("detect_objects") as span:
        start = time.time()
        span.add_event("loadImagesToGPU")
        inputs = batch.to(device)

        with tracer.start_as_current_span("model_inference"):
            outputs = model(**inputs)

        logits_per_image = outputs.logits_per_image
        probs = logits_per_image.softmax(dim=1).cpu().numpy()

        processing_time = time.time() - start
        span.set_attribute("processing_time", processing_time)

        batch_detection_results: List[Tuple[ImageMeta, List[DetectionResult]]] = []

        for idx, result in enumerate(probs):
            image_meta, _ = image_pairs[idx]
            detection_results = []
            for i, (label, score) in enumerate(zip(labels, result)):
                detection_results.append(DetectionResult(label=label, confidence=score))
            batch_detection_results.append((image_meta, detection_results))

        return batch_detection_results
