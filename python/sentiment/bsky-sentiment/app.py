import logging
import os
from time import time

from fastapi import FastAPI, Request
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBasedTraceIdRatio
from prometheus_fastapi_instrumentator import Instrumentator
from pythonjsonlogger import jsonlogger
from starlette.middleware.base import BaseHTTPMiddleware

from .models import Decision, Post, PostList
from .sentiment import get_sentiment

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
    resource = Resource(attributes={SERVICE_NAME: "bsky-sentiment"})
    trace.set_tracer_provider(
        TracerProvider(resource=resource, sampler=ParentBasedTraceIdRatio(0.2))
    )
    trace.get_tracer_provider().add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(
                endpoint=otel_endpoint + "v1/traces",
            )
        )
    )

app = FastAPI()

# Instrument FastAPI for OpenTelemetry
if otel_endpoint:
    FastAPIInstrumentor.instrument_app(app)

# Instrument FastAPI for Prometheus
Instrumentator().instrument(
    app,
    latency_lowr_buckets=[0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.5, 5, 10],
).expose(app, include_in_schema=False)

# Add logging middleware
app.add_middleware(LoggingMiddleware)


@app.post("/analyze_sentiment", response_model=PostList)
async def analyze_sentiment(posts: PostList):
    for post in posts.posts:
        sentiment, confidence_score = get_sentiment(post.text)
        post.decision = Decision(
            sentiment=sentiment, confidence_score=float(confidence_score)
        )
    return posts
