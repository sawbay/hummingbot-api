import logging
import time
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

logger = logging.getLogger(__name__)

router = APIRouter(prefix="", tags=["Health"])

@router.get("/health")
async def health_check(request: Request):
    """
    Health check endpoint to verify that the API and its dependencies are running.
    Checks:
    - Database connectivity
    - MQTT broker connection
    - Docker daemon availability
    """
    status = {}
    
    # 1. Database Check
    try:
        async with request.app.state.db_manager.get_session_context() as session:
            await session.execute(text("SELECT 1"))
            status["database"] = "ok"
    except Exception as e:
        status["database"] = f"error: {str(e)}"
    
    # 2. MQTT Check
    try:
        mqtt_manager = request.app.state.bots_orchestrator.mqtt_manager
        status["mqtt"] = "ok" if mqtt_manager.is_connected() else "disconnected"
    except Exception as e:
        status["mqtt"] = f"error: {str(e)}"
        
    # 3. Docker Check
    try:
        docker_service = request.app.state.docker_service
        status["docker"] = "ok" if docker_service.is_docker_running() else "unavailable"
    except Exception as e:
        status["docker"] = f"error: {str(e)}"
    
    # Calculate uptime
    uptime_seconds = time.time() - getattr(request.app.state, "start_time", time.time())
    
    # Determine status code
    all_ok = all(v == "ok" for v in status.values())
    status_code = 200 if all_ok else 503
    
    return JSONResponse(
        status_code=status_code,
        content={
            "status": status,
            "uptime_seconds": round(uptime_seconds, 1)
        }
    )
