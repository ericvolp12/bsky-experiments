import asyncio
import base64
import io
import logging
import os
from time import time
from typing import AsyncGenerator, List, Optional, Tuple

import aioredis
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from PIL import Image
from prometheus_client import start_http_server, Counter
from pythonjsonlogger import jsonlogger

from .models import ImageMeta, ImageResult
from .object_detection import DetectionResult, detect_objects, processor

# Set up JSON logging
formatter = jsonlogger.JsonFormatter()
handler = logging.StreamHandler()

# Use OUR `formatter` to format all `logging` entries.
handler.setFormatter(formatter)
root_logger = logging.getLogger()
root_logger.addHandler(handler)
root_logger.setLevel(logging.INFO)

# Set up OpenTelemetry
otel_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
if otel_endpoint:
    resource = Resource(attributes={SERVICE_NAME: "bsky-object-detection"})
    tp = TracerProvider(resource=resource)
    tp.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(
                endpoint=otel_endpoint + "v1/traces",
            )
        )
    )
    trace.set_tracer_provider(tp)

images_processed_successfully = Counter(
    "images_processed_successfully", "Number of images processed successfully"
)
images_failed = Counter("images_failed", "Number of images failed")
images_submitted = Counter("images_submitted", "Number of images submitted")

tracer = trace.get_tracer("bsky-object-detection")


async def decode_image(
    image_meta: ImageMeta,
) -> Tuple[ImageMeta, Optional[Image.Image]]:
    try:
        # Decode the image from the base64 string
        image = Image.open(io.BytesIO(base64.b64decode(image_meta.data)))
        return image_meta, image
    except Exception as e:
        logging.error(
            f"Error decoding image: {e}",
            extra={"error": e, "image_meta": image_meta},
        )
        return image_meta, None


async def batched_images(
    image_metas: List[ImageMeta], batch_size: int
) -> AsyncGenerator[List[Tuple[ImageMeta, Optional[Image.Image]]], None]:
    tasks = [decode_image(img) for img in image_metas]
    buffer = []

    for coro in asyncio.as_completed(tasks):
        result = await coro
        buffer.append(result)
        if len(buffer) >= batch_size:
            yield buffer
            buffer = []

    # Yield any remaining images in the buffer
    if buffer:
        yield buffer


# Setup environment variables
REDIS_URL = os.getenv("REDIS_STREAM_URL", "redis://localhost:6379")
IMAGE_STREAM = os.getenv("IMAGE_STREAM", "object_detection_images")
RESULT_TOPIC = os.getenv("RESULT_TOPIC", "object_detection_results")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "8"))


async def preprocess_and_detect(
    image_pairs: List[Tuple[ImageMeta, Image.Image]]
) -> List[
    Tuple[ImageMeta, List[DetectionResult]]
]:  # Replace 'any' with the actual type of your detection result
    detection_results = []
    if image_pairs:
        try:

            def preprocess_batch():
                with tracer.start_as_current_span("preprocess_images"):
                    batch = processor(
                        images=[img for _, img in image_pairs], return_tensors="pt"
                    )
                    return batch

            batch = await asyncio.to_thread(preprocess_batch)
            detection_results = detect_objects(batch=batch, image_pairs=image_pairs)
        except Exception as e:
            logging.error(
                f"Error detecting objects: {e}",
                extra={"error": e, "successful": image_pairs},
            )
    return detection_results


async def fetch_and_batch_images(
    redis: aioredis.Redis, batch_size: int
) -> AsyncGenerator[List[ImageMeta], None]:
    last_id = "0-0"  # Starting point for the stream
    while True:
        streams = await redis.xread(
            {IMAGE_STREAM: last_id}, count=batch_size, block=1000
        )
        if streams:
            messages = streams[0][1]
            if messages:
                image_metas = [ImageMeta(**message[1]) for _, message in messages]
                yield image_metas
                last_id = messages[-1][0]


async def process_images(redis: aioredis.Redis):
    async for image_metas in fetch_and_batch_images(redis, BATCH_SIZE):
        start = time()
        pil_images = await asyncio.gather(
            *[decode_image(img_meta) for img_meta in image_metas]
        )
        successful = [
            (img_meta, pil_image) for img_meta, pil_image in pil_images if pil_image
        ]
        detection_results = await preprocess_and_detect(successful)
        # Publish results to the result topic
        for image_meta, detection in detection_results:
            await redis.xadd(
                RESULT_TOPIC, ImageResult(meta=image_meta, results=detection).to_dict()
            )
        images_processed_successfully.inc(len(detection_results))
        images_processed_time = time() - start
        logging.info(
            f"Processed {len(detection_results)} images in {images_processed_time:.3f} seconds"
        )


async def main():
    redis = await aioredis.from_url(REDIS_URL)
    try:
        await process_images(redis)
    finally:
        await redis.close()


if __name__ == "__main__":
    # Start up the server to expose the metrics.
    start_http_server(8093)
    # Your async run command or other setup logic here
    asyncio.run(main())
