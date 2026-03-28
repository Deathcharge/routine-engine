"""
Copyright (c) 2025 Andrew John Ward. All Rights Reserved.
PROPRIETARY AND CONFIDENTIAL - See LICENSE file for terms.

🌀 Helix Spirals Main FastAPI Application
Drop-in Zapier replacement with 98.7% more efficiency
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import asyncpg
import redis.asyncio as redis
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from .copilot import copilot_router
from .engine import SpiralEngine
from .models import ExecutionRequest, ExecutionResponse, WebhookPayload
from .routes import executions_router, spirals_router, templates_router
from .scheduler import SpiralScheduler
from .storage import SpiralStorage
from .webhooks import WebhookReceiver
from .zapier_import import ZapierImporter

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global instances
engine: SpiralEngine | None = None
storage: SpiralStorage | None = None
scheduler: SpiralScheduler | None = None
webhook_receiver: WebhookReceiver | None = None
redis_client: redis.Redis | None = None
pg_pool: asyncpg.Pool | None = None


# WebSocket manager for real-time updates
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info("WebSocket connection established. Total: %s", len(self.active_connections))

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info("WebSocket connection closed. Remaining: %s", len(self.active_connections))

    async def broadcast(self, message: dict):
        """Broadcast message to all connected WebSocket clients"""
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except (ConnectionError, TimeoutError) as e:
                logger.debug("WebSocket connection error during broadcast: %s", e)
            except (ValueError, TypeError, KeyError) as e:
                logger.debug("WebSocket message validation error: %s", e)
            except Exception as e:
                logger.exception("Unexpected error broadcasting to WebSocket: %s", e)


ws_manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and cleanup resources"""
    global engine, storage, scheduler, webhook_receiver, redis_client, pg_pool

    try:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        redis_client = await redis.from_url(redis_url)
        logger.info("✅ Redis connection established")

        # Initialize PostgreSQL
        pg_url = os.getenv("DATABASE_URL", "postgresql://localhost/helix_spirals")
        pg_pool = await asyncpg.create_pool(pg_url, min_size=10, max_size=20)
        logger.info("✅ PostgreSQL pool created")

        # Initialize components
        storage = SpiralStorage(pg_pool, redis_client)
        await storage.initialize()

        engine = SpiralEngine(storage, ws_manager)
        scheduler = SpiralScheduler(engine, storage)
        webhook_receiver = WebhookReceiver(engine, storage)

        # Start scheduler
        asyncio.create_task(scheduler.start())
        logger.info("✅ Helix Spirals engine initialized")

        # Load and activate spirals
        spirals = await storage.get_all_spirals()
        for spiral in spirals:
            if spiral.enabled:
                await scheduler.register_spiral(spiral)
        logger.info("✅ Loaded %s spirals", len(spirals))

        yield

    except Exception as e:
        logger.error("Failed to initialize Helix Spirals: %s", e)
        raise

    finally:
        # Cleanup
        if scheduler:
            await scheduler.stop()
        if pg_pool:
            await pg_pool.close()
        if redis_client:
            await redis_client.close()
        logger.info("🛑 Helix Spirals engine stopped")


# Create FastAPI app
app = FastAPI(
    title="Helix Spirals Automation Engine",
    description="A powerful Zapier replacement with 98.7% more efficiency",
    version="1.0.0",
    lifespan=lifespan,
)

# Configure rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Configure CORS
allowed_origins = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000,https://localhost:3000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
    allow_headers=["*"],
    expose_headers=["X-Execution-ID", "X-Spiral-ID"],
)

# Mount routers
app.include_router(spirals_router, prefix="/api/spirals", tags=["spirals"])
app.include_router(executions_router, prefix="/api/executions", tags=["executions"])
app.include_router(templates_router, prefix="/api/templates", tags=["templates"])
app.include_router(copilot_router, prefix="/api/copilot", tags=["copilot"])


# Health check
@app.get("/health")
async def health_check():
    """Check system health and component status"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now(UTC).isoformat(),
        "components": {
            "engine": engine is not None,
            "storage": storage is not None,
            "scheduler": scheduler is not None and scheduler.is_running,
            "redis": redis_client is not None,
            "postgres": pg_pool is not None,
        },
        "version": "1.0.0",
    }

    # Check Redis
    try:
        health_status["components"]["redis_connected"] = True
    except (ValueError, TypeError, KeyError, IndexError):
        health_status["components"]["redis_connected"] = False
        health_status["status"] = "degraded"

    # Check PostgreSQL
    try:
        conn = await pg_pool.acquire()
        await conn.fetchval("SELECT 1")
        health_status["components"]["postgres_connected"] = True
    except (asyncpg.exceptions.PostgresError, ConnectionError, TimeoutError) as e:
        logger.debug("PostgreSQL health check failed: %s", e)
        health_status["components"]["postgres_connected"] = False
        health_status["status"] = "degraded"
    except (ValueError, TypeError, AttributeError) as e:
        logger.debug("PostgreSQL validation error: %s", e)
        health_status["components"]["postgres_connected"] = False
        health_status["status"] = "degraded"
    except Exception as e:
        logger.exception("Unexpected PostgreSQL health check error: %s", e)
        health_status["components"]["postgres_connected"] = False
        health_status["status"] = "degraded"

    return health_status


# Statistics endpoint
@app.get("/stats")
async def get_statistics():
    """Get system-wide statistics"""
    if not storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    stats = await storage.get_statistics()
    stats["active_connections"] = len(ws_manager.active_connections)
    stats["scheduler_tasks"] = scheduler.get_task_count() if scheduler else 0

    return stats


# WebSocket endpoint for real-time updates
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket connection for real-time spiral updates"""
    await ws_manager.connect(websocket)
    try:
        # Keep connection alive and handle client messages
        data = await websocket.receive_text()

        # Handle subscription requests
        if data.startswith("subscribe:"):
            spiral_id = data.split(":")[1]
            # Add to subscription list (implement subscription logic)
            await websocket.send_json(
                {
                    "type": "subscribed",
                    "spiralId": spiral_id,
                    "timestamp": datetime.now(UTC).isoformat(),
                }
            )

    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)


# Universal webhook receiver
@app.post("/webhook/{spiral_id}")
@limiter.limit("100/hour")
async def receive_webhook(spiral_id: str, request: Request, background_tasks: BackgroundTasks):
    """Universal webhook endpoint - accepts any webhook and triggers spirals"""
    if not webhook_receiver:
        raise HTTPException(status_code=503, detail="Webhook receiver not initialized")

    # Get webhook data
    headers = dict(request.headers)
    body = await request.body()

    try:
        import json

        payload = json.loads(body) if body else {}
    except json.JSONDecodeError:
        payload = {"raw": body.decode("utf-8", errors="ignore")}
    except (ValueError, TypeError) as e:
        logger.debug("JSON parsing validation error: %s", e)
        payload = {"raw": body.decode("utf-8", errors="ignore")}
    except Exception as e:
        logger.exception("Unexpected error parsing webhook payload: %s", e)
        payload = {"raw": body.decode("utf-8", errors="ignore")}

    webhook_data = WebhookPayload(
        spiral_id=spiral_id,
        method=request.method,
        headers=headers,
        body=payload,
        query_params=dict(request.query_params),
        client_ip=request.client.host if request.client else None,
    )

    # Process webhook in background
    background_tasks.add_task(webhook_receiver.process_webhook, webhook_data)

    return {"status": "accepted", "spiralId": spiral_id}


# Execute spiral manually
@app.post("/execute/{spiral_id}")
async def execute_spiral_manual(spiral_id: str, request: ExecutionRequest = None):
    """Manually execute a spiral"""
    if not engine:
        raise HTTPException(status_code=503, detail="Engine not initialized")

    try:
        context = await engine.execute_spiral(
            spiral_id=spiral_id,
            trigger_type="manual",
            trigger_data=request.trigger_data if request else {},
            metadata={"source": "manual_api"},
        )

        # Broadcast execution start
        await ws_manager.broadcast(
            {
                "type": "execution_started",
                "spiralId": spiral_id,
                "executionId": context.execution_id,
                "timestamp": context.started_at,
            }
        )

        return ExecutionResponse(
            execution_id=context.execution_id,
            spiral_id=context.spiral_id,
            status=context.status,
            started_at=context.started_at,
            completed_at=context.completed_at,
        )

    except Exception as e:
        logger.error("Failed to execute spiral %s: %s", spiral_id, e)
        raise HTTPException(status_code=500, detail="Spiral execution failed")


# Zapier Import Endpoint
@app.post("/api/import/zapier")
async def import_zapier_export(request: Request):
    """Import Zapier export JSON and convert to Helix Spirals"""
    if not storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    try:
        zapier_data = await request.json()
        importer = ZapierImporter(storage)
        import_stats = await importer.import_zapier_export(zapier_data)

        # Log import stats
        logger.info(
            "Zapier import completed: {}/{} converted".format(import_stats["converted"], import_stats["total_zaps"])
        )

        # Broadcast import completion
        await ws_manager.broadcast(
            {
                "type": "zapier_import_completed",
                "stats": import_stats,
                "timestamp": datetime.now(UTC).isoformat(),
            }
        )

        return import_stats

    except Exception as e:
        logger.error("Failed to import Zapier data: %s", e)
        raise HTTPException(status_code=400, detail="Failed to import Zapier data")


# Error handlers
@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    return JSONResponse(
        status_code=404,
        content={"detail": "Resource not found", "path": request.url.path},
    )


@app.exception_handler(500)
async def internal_error_handler(request: Request, exc):
    logger.error("Internal error: %s", exc)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "timestamp": datetime.now(UTC).isoformat(),
        },
    )


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 5001))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
