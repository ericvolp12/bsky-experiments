import asyncio
import io
import logging
import os
from time import time
from typing import AsyncGenerator, List, Optional, Tuple

import aiohttp

# import pyroscope
from fastapi import FastAPI, Request
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from PIL import Image
from prometheus_client import Counter
from prometheus_fastapi_instrumentator import Instrumentator
from pythonjsonlogger import jsonlogger
from starlette.middleware.base import BaseHTTPMiddleware

from .models import ImageMeta, ImageResult
from .object_detection import DetectionResult, detect_objects, processor, imageClasses

# Set up JSON logging
formatter = jsonlogger.JsonFormatter()
handler = logging.StreamHandler()

# Use OUR `formatter` to format all `logging` entries.
handler.setFormatter(formatter)
root_logger = logging.getLogger()
root_logger.addHandler(handler)
root_logger.setLevel(logging.INFO)

for _log in ["uvicorn", "uvicorn.error"]:
    # Clear the log handlers for uvicorn loggers, and enable propagation
    # so the messages are caught by our root logger and formatted correctly
    # by structlog
    logging.getLogger(_log).handlers.clear()
    logging.getLogger(_log).propagate = True

# Since we re-create the access logs ourselves, to add all information
# in the structured log, we clear the handlers and prevent the logs to propagate to
# a logger higher up in the hierarchy (effectively rendering them silent).
logging.getLogger("uvicorn.access").handlers.clear()
logging.getLogger("uvicorn.access").propagate = False


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time()
        response = await call_next(request)
        process_time = time() - start_time
        logging.info(
            {
                "message": "request handled",
                "path": request.url.path,
                "method": request.method,
                "processing_time": process_time,
                "status_code": response.status_code,
                "query_params": request.query_params,
            },
        )
        return response


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

# Set up Pyroscope Continuous Profiler (Disabling for now because of big spike in CPU usage when running)
# pyroscope_endpoint = os.getenv("PYROSCOPE_SERVER_ADDRESS")
# if pyroscope_endpoint:
#     pyroscope.configure(
#         server_address=pyroscope_endpoint,  # pyroscope server url
#         application_name="bsky-object-detection",  # name of your application
#     )


app = FastAPI()

# Instrument FastAPI for OpenTelemetry
if otel_endpoint:
    FastAPIInstrumentor.instrument_app(app)

# Instrument FastAPI for Prometheus
Instrumentator().instrument(
    app,
    latency_lowr_buckets=[0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.5, 5, 10, 20, 30, 60],
).expose(app, include_in_schema=False)

# Add logging middleware
app.add_middleware(LoggingMiddleware)

images_processed_successfully = Counter(
    "images_processed_successfully", "Number of images processed successfully"
)
images_failed = Counter("images_failed", "Number of images failed")
images_submitted = Counter("images_submitted", "Number of images submitted")

tracer = trace.get_tracer("bsky-object-detection")

AioHttpClientInstrumentor().instrument()


# Async function to download an image
async def download_image(
    session: aiohttp.ClientSession, image_meta: ImageMeta
) -> Tuple[ImageMeta, Image.Image | None]:
    with tracer.start_as_current_span("download_image") as span:
        span.add_event("Download image")
        async with session.get(image_meta.url) as resp:
            # If the response is not 200, log an error and continue to the next image
            if resp.status != 200:
                logging.error(
                    f"Error fetching image from {image_meta.url} - {resp.status}"
                )
                span.set_attribute("error", True)
                span.set_attribute(
                    "error.message", f"Error fetching image: {resp.status}"
                )
                return image_meta, None
            span.add_event("Read image data")
            imageData = await resp.read()
            img = Image.open(io.BytesIO(imageData)).convert("RGB")
            if img.width < 100 or img.height < 100:
                logging.error(
                    f"Image from {image_meta.url} is too small ({img.width}x{img.height})"
                )
                span.set_attribute("error", True)
                span.set_attribute("error.message", "Image too small")
                return image_meta, None
            return image_meta, img


async def batched_downloads(
    session: aiohttp.ClientSession, image_metas: List[ImageMeta], batch_size: int
) -> AsyncGenerator[List[Tuple[ImageMeta, Optional[Image.Image]]], None]:
    tasks = [download_image(session, img) for img in image_metas]
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


batch_size = 8
batch_size_str = os.getenv("BATCH_SIZE")
if batch_size_str:
    batch_size = int(batch_size_str)


async def preprocess_and_detect(
    image_pairs: List[Tuple[ImageMeta, Image.Image]]
) -> List[
    Tuple[ImageMeta, List[DetectionResult]]
]:  # Replace 'any' with the actual type of your detection result
    detection_results = []
    if image_pairs:
        try:

            def preprocess_batch():
                with tracer.start_as_current_span("preprocess_images") as span:
                    batch = processor(
                        text=imageClasses,
                        images=[img for _, img in image_pairs],
                        return_tensors="pt",
                        Padding=True,
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


@app.post("/detect_objects", response_model=List[ImageResult])
async def detect_objects_endpoint(image_metas: List[ImageMeta]):
    image_results: List[ImageResult] = []
    images_submitted.inc(len(image_metas))

    detection_tasks = []
    async with aiohttp.ClientSession() as session:
        async for pil_images in batched_downloads(session, image_metas, batch_size):
            # Separate successful downloads for processing
            successful = [
                (image_meta, pil_image)
                for image_meta, pil_image in pil_images
                if pil_image
            ]

            # Initiate detection on successful downloads in the background
            if successful:
                task = asyncio.create_task(preprocess_and_detect(successful))
                detection_tasks.append(task)

            # Immediately handle failed downloads
            for img_pair in pil_images:
                if not img_pair[1]:  # No image returned
                    images_failed.inc()
                    image_results.append(ImageResult(meta=img_pair[0], results=[]))

    # Await all detection tasks
    for task in detection_tasks:
        detection_result = await task
        for image_meta, detection in detection_result:
            images_processed_successfully.inc()
            image_results.append(ImageResult(meta=image_meta, results=detection))

    return image_results
