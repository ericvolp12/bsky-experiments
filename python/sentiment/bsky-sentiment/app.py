import logging
from time import time

from fastapi import FastAPI, Request
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
from prometheus_fastapi_instrumentator import Instrumentator
from pythonjsonlogger import jsonlogger
from starlette.middleware.base import BaseHTTPMiddleware

from .models import Decision, Post, PostList
from .sentiment import get_sentiment


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time()
        response = await call_next(request)
        process_time = (time() - start_time) * 1000
        log.info(
            {
                "path": request.url.path,
                "method": request.method,
                "processing_time": process_time,
                "status_code": response.status_code,
            }
        )
        return response


# Set up JSON logging
log = logging.getLogger("uvicorn.error")
if log.hasHandlers():
    for handler in log.handlers:
        log.removeHandler(handler)
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
log.addHandler(logHandler)

# Set up OpenTelemetry
# trace.set_tracer_provider(TracerProvider())
# trace.get_tracer_provider().add_span_processor(
#     SimpleSpanProcessor(ConsoleSpanExporter())
# )

app = FastAPI()

# Instrument FastAPI for OpenTelemetry
# FastAPIInstrumentor.instrument_app(app)

# Instrument FastAPI for Prometheus
Instrumentator().instrument(app).expose(app, include_in_schema=False)

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
