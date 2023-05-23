import asyncio
import io
import logging
import os
from time import time
from typing import List

import aiohttp

# import pyroscope
from fastapi import FastAPI, Request
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
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
from .object_detection import detect_objects

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
    trace.set_tracer_provider(TracerProvider(resource=resource))
    trace.get_tracer_provider().add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(
                endpoint=otel_endpoint + "v1/traces",
            )
        )
    )

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


# Async function to download an image
async def download_image(
    session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, image_meta: ImageMeta
) -> Image.Image:
    tracer = trace.get_tracer("bsky-object-detection")
    with tracer.start_as_current_span("download_image") as span:
        span.add_event("Wait for semaphore")
        async with semaphore:
            span.add_event("Acquired semaphore")
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
                    return None
                span.add_event("Read image data")
                imageData = await resp.read()
                return Image.open(io.BytesIO(imageData))


@app.post("/detect_objects", response_model=List[ImageResult])
async def detect_objects_endpoint(image_metas: List[ImageMeta]):
    images_submitted.inc(len(image_metas))
    image_results: List[ImageResult] = []

    # Create a semaphore with a limit of 10 concurrent tasks
    semaphore = asyncio.Semaphore(10)

    # Grab all the images first and convert them to PIL images
    async with aiohttp.ClientSession() as session:
        tasks = [
            download_image(session, semaphore, image_meta) for image_meta in image_metas
        ]
        pil_images = await asyncio.gather(*tasks)

    # Separate successfully downloaded images and failed ones
    successful_images = [img for img in pil_images if img is not None]
    failed_indices = [i for i, img in enumerate(pil_images) if img is None]

    # Run the object detection model on successful images
    all_batch_detection_results = []
    for i in range(0, len(successful_images), 10):
        image_batch = successful_images[i : i + 10]
        try:
            batch_detection_results = detect_objects(image_batch)
        except Exception as e:
            logging.error(f"Error running object detection model: {e}")
            batch_detection_results = [[] for _ in image_batch]  # Create empty results
        all_batch_detection_results.extend(batch_detection_results)

    # Populate the image results
    succ_idx = 0
    for idx, image_meta in enumerate(image_metas):
        if idx in failed_indices:
            image_results.append(ImageResult(meta=image_meta, results=[]))
            images_failed.inc()
        else:
            image_results.append(
                ImageResult(
                    meta=image_meta, results=all_batch_detection_results[succ_idx]
                )
            )
            succ_idx += 1
            images_processed_successfully.inc()

    return image_results
