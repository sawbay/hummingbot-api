from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse

from deps import get_r2_storage_service
from services.r2_storage_service import R2BotsStorageService

router = APIRouter(tags=["Storage"], prefix="/storage/r2")


@router.get("/status")
async def get_r2_status(storage_service: R2BotsStorageService = Depends(get_r2_storage_service)):
    """Return R2 durable bots storage configuration and latest async sync job state."""
    return {"status": "success", "data": storage_service.status()}


@router.post("/pull")
async def pull_from_r2(storage_service: R2BotsStorageService = Depends(get_r2_storage_service)):
    """Start an async pull of durable bots prefixes from R2 into the local bots directory."""
    try:
        started, job = storage_service.start_background_sync("pull")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED if started else status.HTTP_409_CONFLICT,
        content={"status": "accepted" if started else "conflict", "data": job},
    )


@router.post("/push")
async def push_to_r2(storage_service: R2BotsStorageService = Depends(get_r2_storage_service)):
    """Start an async push of durable local bots prefixes to R2."""
    try:
        started, job = storage_service.start_background_sync("push")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED if started else status.HTTP_409_CONFLICT,
        content={"status": "accepted" if started else "conflict", "data": job},
    )
