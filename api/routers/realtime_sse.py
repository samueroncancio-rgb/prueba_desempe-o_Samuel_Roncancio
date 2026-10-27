import asyncio
import json
import logging
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse

from api.auth.jwt_handler import require_viewer
from api.schemas.pydantic_models import UserProfile
from api.db import get_db_cursor

router = APIRouter(prefix="/api", tags=["Real-time Telemetry"])
logger = logging.getLogger("sismowatch.sse")

@router.get("/stream")
async def event_stream(request: Request, user: UserProfile = Depends(require_viewer)):
    """
    Server-Sent Events (SSE) telemetry stream.
    Emits latest seismic updates, status heartbeat, and reconnect instructions.
    """
    async def sse_generator():
        last_checked = datetime.now(timezone.utc)
        # Immediate welcome event
        yield f"event: connect\ndata: {json.dumps({'status': 'CONNECTED', 'user': user.username, 'timestamp': last_checked.isoformat()})}\n\n"

        while True:
            if await request.is_disconnected():
                logger.info(f"SSE client {user.username} disconnected.")
                break

            try:
                # Poll gold.fact_earthquake_events for events updated since last cycle
                with get_db_cursor() as cur:
                    cur.execute("""
                        SELECT event_id, mag, place_sanitized, event_time,
                               longitude, latitude, depth_km, tsunami_alert
                        FROM gold.fact_earthquake_events
                        WHERE updated_time >= %s
                        ORDER BY updated_time DESC
                        LIMIT 10;
                    """, (last_checked,))
                    new_events = cur.fetchall()

                last_checked = datetime.now(timezone.utc)

                if new_events:
                    payload = [
                        {
                            "event_id": e["event_id"],
                            "mag": float(e["mag"]),
                            "place": e["place_sanitized"],
                            "time": e["event_time"].isoformat(),
                            "lat": float(e["latitude"]),
                            "lon": float(e["longitude"]),
                            "tsunami": e["tsunami_alert"]
                        }
                        for e in new_events
                    ]
                    yield f"event: seismic_update\ndata: {json.dumps(payload)}\n\n"
                else:
                    # Heartbeat keep-alive every 10 seconds
                    yield f"event: heartbeat\ndata: {json.dumps({'alive': True, 'timestamp': last_checked.isoformat()})}\n\n"

            except Exception as e:
                logger.warning(f"SSE polling error: {e}")
                yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"

            await asyncio.sleep(5)

    return StreamingResponse(
        sse_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )
