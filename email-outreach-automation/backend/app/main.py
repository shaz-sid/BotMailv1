import logging
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from config import settings
from database import create_tables, engine
from routes.analytics import router as analytics_router
from routes.campaigns import router as campaigns_router
from routes.contacts import router as contacts_router
from routes.email import router as emails_router

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level    = logging.INFO if not settings.DEBUG else logging.DEBUG,
    format   = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt  = "%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan  (startup / shutdown)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- startup ---
    logger.info("Starting %s v%s", settings.APP_NAME, settings.APP_VERSION)

    try:
        create_tables()
        logger.info("Database tables verified.")
    except SQLAlchemyError as exc:
        logger.critical("Database initialisation failed: %s", exc)
        raise

    # confirm broker reachability (non-fatal warning only)
    try:
        from workers.celery_worker import celery
        celery.connection().ensure_connection(max_retries=3)
        logger.info("Celery broker reachable.")
    except Exception as exc:
        logger.warning("Celery broker not reachable at startup: %s", exc)

    logger.info("Application ready.")
    yield

    # --- shutdown ---
    logger.info("Shutting down — closing DB connections.")
    engine.dispose()
    logger.info("Shutdown complete.")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app() -> FastAPI:
    app = FastAPI(
        title       = settings.APP_NAME,
        version     = settings.APP_VERSION,
        description = (
            "AI-powered email outreach platform. "
            "Manage contacts, build campaigns, generate personalised emails "
            "with Gemini, and track engagement — all via REST."
        ),
        docs_url    = "/docs"    if settings.DEBUG else None,
        redoc_url   = "/redoc"   if settings.DEBUG else None,
        lifespan    = lifespan,
    )

    _register_middleware(app)
    _register_routers(app)
    _register_exception_handlers(app)

    return app


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

def _register_middleware(app: FastAPI) -> None:

    app.add_middleware(
        CORSMiddleware,
        allow_origins     = settings.ALLOWED_ORIGINS,
        allow_credentials = True,
        allow_methods     = ["*"],
        allow_headers     = ["*"],
    )

    app.add_middleware(GZipMiddleware, minimum_size=1000)

    @app.middleware("http")
    async def request_logger(request: Request, call_next):
        start   = time.perf_counter()
        response = await call_next(request)
        duration = (time.perf_counter() - start) * 1000

        logger.info(
            "%s %s → %s  (%.1f ms)",
            request.method,
            request.url.path,
            response.status_code,
            duration,
        )
        response.headers["X-Response-Time-Ms"] = f"{duration:.1f}"
        return response


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

def _register_routers(app: FastAPI) -> None:
    prefix = settings.API_PREFIX   # e.g. "/api/v1"

    app.include_router(contacts_router,  prefix=prefix)
    app.include_router(campaigns_router, prefix=prefix)
    app.include_router(emails_router,    prefix=prefix)
    app.include_router(analytics_router, prefix=prefix)

    # --- meta routes (no version prefix) ---

    @app.get("/", include_in_schema=False)
    def root():
        return {
            "app":     settings.APP_NAME,
            "version": settings.APP_VERSION,
            "docs":    "/docs" if settings.DEBUG else "disabled in production",
        }

    @app.get("/health", tags=["Meta"])
    def health() -> dict[str, Any]:
        """Liveness probe — confirms the API process is alive."""
        return {"status": "ok"}

    @app.get("/ready", tags=["Meta"])
    def readiness() -> dict[str, Any]:
        """
        Readiness probe — confirms DB connectivity before accepting traffic.
        Returns 503 if the database is unreachable.
        """
        try:
            with engine.connect() as conn:
                conn.execute(__import__("sqlalchemy").text("SELECT 1"))
            db_status = "ok"
        except SQLAlchemyError as exc:
            logger.error("Readiness check failed: %s", exc)
            return JSONResponse(
                status_code = status.HTTP_503_SERVICE_UNAVAILABLE,
                content     = {"status": "unavailable", "detail": str(exc)},
            )

        return {"status": "ready", "database": db_status}


# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------

def _register_exception_handlers(app: FastAPI) -> None:

    @app.exception_handler(SQLAlchemyError)
    async def sqlalchemy_handler(request: Request, exc: SQLAlchemyError):
        logger.error("Unhandled DB error on %s: %s", request.url.path, exc)
        return JSONResponse(
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
            content     = {"detail": "A database error occurred. Please try again."},
        )

    @app.exception_handler(Exception)
    async def generic_handler(request: Request, exc: Exception):
        logger.critical("Unhandled exception on %s: %s", request.url.path, exc, exc_info=True)
        return JSONResponse(
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
            content     = {"detail": "An unexpected error occurred."},
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host    = "0.0.0.0",
        port    = settings.PORT,
        reload  = settings.DEBUG,
        workers = 1 if settings.DEBUG else settings.WORKERS,
        log_level = "debug" if settings.DEBUG else "info",
    )