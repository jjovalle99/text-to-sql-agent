from typing import Annotated

import psycopg
from fastapi import APIRouter, Depends, HTTPException, status

from ..dependencies import get_db_connection

router = APIRouter(tags=["health"])


@router.get("/liveness", status_code=status.HTTP_200_OK)
async def check_health_liveness() -> dict[str, str]:
    """Liveness probe.

    Returns:
        Status message indicating the service is alive
    """
    return {"status": "ok"}


@router.get("/startup", status_code=status.HTTP_200_OK)
async def health_startup(
    db_conn: Annotated[psycopg.AsyncConnection, Depends(get_db_connection)],
) -> dict[str, str]:
    """Startup probe.

    Returns:
        Status message indicating the service is ready
    """
    try:
        await db_conn.execute("SELECT 1")
        return {"status": "ok", "database": "connected"}

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Service not ready: {e}",
        )
