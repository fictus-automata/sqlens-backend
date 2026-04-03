import uuid

import structlog
from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware

from app.api.query_routes import router as api_router
from app.api.schema_routes import router as schema_router
from app.api.graph_routes import router as graph_router
from app.core.config import settings
from app.core.logging import configure_logging, get_logger

configure_logging(settings.log_level)
log = get_logger(__name__)


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Attach a request-scoped ID to every request and propagate it to:
    - The structlog context (so all log lines within the request carry it).
    - The response header (so callers can correlate client-side).
    """

    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get(settings.request_id_header) or uuid.uuid4().hex
        request.state.request_id = request_id
        # Clear any stale context from a previous request on this thread/task,
        # then bind the fresh request_id for this request's lifetime.
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id)
        
        log.info("api.request.start", method=request.method, url=str(request.url))
        import time
        start_time = time.perf_counter()
        
        response = await call_next(request)
        
        process_time = time.perf_counter() - start_time
        response.headers[settings.request_id_header] = request_id
        
        log_msg = "api.request.complete" if response.status_code < 400 else "api.request.error"
        log.info(
            log_msg,
            method=request.method,
            url=str(request.url),
            status_code=response.status_code,
            duration_ms=round(process_time * 1000, 2)
        )
        
        return response


app = FastAPI(title="Altimate SQL Query Service")
app.add_middleware(RequestIDMiddleware)
app.include_router(api_router)
app.include_router(schema_router)
app.include_router(graph_router)


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}

