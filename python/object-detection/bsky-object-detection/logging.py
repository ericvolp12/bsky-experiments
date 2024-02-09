from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
import logging
from pythonjsonlogger import jsonlogger
from time import time

# Set up JSON logging
log = logging.getLogger("uvicorn.error")
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
log.addHandler(logHandler)


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time()
        response = await call_next(request)
        process_time = (time() - start_time) * 1000
        log.info(
            {
                "path": request.url.path,
                "method": request.method,
                "processing_time": f"{process_time:.2f}ms",
                "status_code": response.status_code,
            }
        )
        return response
