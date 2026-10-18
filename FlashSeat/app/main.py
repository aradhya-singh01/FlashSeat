"""
FlashSeat - High-Concurrency Flash Ticket Booking Engine
Main FastAPI application file.

Handles:
- Real-time seat map querying with Redis-overlayed availability states
- High-concurrency seat hold reservations with distributed Redis TTL locking
- ACID-safe booking confirmation with optimistic concurrency control
- Anti-scalping rate limiting
- Demo seeding and test reset endpoints
"""

import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import (
    Booking,
    Event,
    Seat,
    get_db,
    init_db,
)
from app.redis_client import redis_client


# ---------------------------------------------------------------------------
# Lifespan Context Manager (Startup / Shutdown)
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initializes Database tables, demo data, and Redis connection pool on startup."""
    # 1. Initialize DB schema and seed demo concert
    await init_db()

    # 2. Connect to Redis (or activate resilient in-memory fallback)
    await redis_client.connect()

    yield

    # 3. Graceful shutdown
    await redis_client.disconnect()


app = FastAPI(
    title="FlashSeat API",
    description="High-concurrency flash sale seat reservation and ticketing engine powered by FastAPI, Redis, and SQLAlchemy.",
    version="1.0.0",
    lifespan=lifespan,
)

# Enable CORS for frontend applications (React/Next.js/Vue/Vanilla)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Resolve and mount static files directory
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(STATIC_DIR, exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# ---------------------------------------------------------------------------
# Pydantic Schemas
# ---------------------------------------------------------------------------
class SeatOut(BaseModel):
    id: int
    seat_number: str
    row: str
    tier: str
    price: float
    status: str  # 'AVAILABLE', 'HELD', 'BOOKED'
    held_by: Optional[str] = None
    ttl_remaining: Optional[int] = None

    class Config:
        from_attributes = True


class EventSummary(BaseModel):
    id: int
    name: str
    description: Optional[str]
    venue: str
    event_time: datetime
    total_seats: int
    available_seats: int
    held_seats: int
    booked_seats: int


class HoldSeatRequest(BaseModel):
    event_id: int = Field(..., description="Target event ID")
    seat_id: int = Field(..., description="Target seat ID to lock")
    user_id: str = Field(..., min_length=1, description="Unique customer/session identifier")


class HoldSeatResponse(BaseModel):
    success: bool
    message: str
    event_id: int
    seat_id: int
    held_by: str
    ttl_seconds: int


class ConfirmBookingRequest(BaseModel):
    event_id: int
    seat_id: int
    user_id: str
    payment_token: Optional[str] = "simulated_token_valid"


class ConfirmBookingResponse(BaseModel):
    success: bool
    message: str
    booking_reference: str
    event_name: str
    seat_number: str
    tier: str
    amount_paid: float
    user_id: str
    confirmed_at: datetime


class ReleaseSeatRequest(BaseModel):
    event_id: int
    seat_id: int
    user_id: str


class ResetResponse(BaseModel):
    message: str
    available_seats_count: int


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=FileResponse, tags=["Frontend"])
async def serve_frontend():
    """Serves the frontend index.html single-page application directly at root path."""
    candidates = [
        os.path.join(STATIC_DIR, "index.html"),
        os.path.join(os.path.dirname(__file__), "static", "index.html"),
        os.path.join(os.path.dirname(__file__), "..", "static", "index.html"),
        os.path.join(os.getcwd(), "app", "static", "index.html"),
        os.path.join(os.getcwd(), "static", "index.html"),
        os.path.join(os.getcwd(), "index.html"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return FileResponse(p)
    return HTMLResponse("<h2>FlashSeat Engine Running</h2><p>index.html not found</p>", status_code=200)


@app.get("/api/health", tags=["System"])
@app.get("/health", tags=["System"])
async def health_check():
    """Engine health status and distributed lock diagnostics."""
    return {
        "engine": "FlashSeat Concurrency Engine",
        "status": "online",
        "redis_status": "connected (distributed)" if redis_client.is_connected else "in-memory resilient mode",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "docs": "/docs",
    }


@app.get("/api/events", response_model=List[EventSummary], tags=["Events"])
async def list_events(db: AsyncSession = Depends(get_db)):
    """Lists all events with dynamic real-time seat availability counts."""
    events_query = await db.execute(select(Event))
    events = events_query.scalars().all()

    summaries = []
    for ev in events:
        # Fetch seats for this event
        seats_query = await db.execute(select(Seat).where(Seat.event_id == ev.id))
        seats = seats_query.scalars().all()

        # Query active Redis holds
        active_holds = await redis_client.get_all_active_holds_for_event(ev.id)

        booked_count = sum(1 for s in seats if s.status == "BOOKED")
        held_count = sum(1 for s in seats if s.id in active_holds and s.status != "BOOKED")
        available_count = len(seats) - booked_count - held_count

        summaries.append(
            EventSummary(
                id=ev.id,
                name=ev.name,
                description=ev.description,
                venue=ev.venue,
                event_time=ev.event_time,
                total_seats=len(seats),
                available_seats=max(available_count, 0),
                held_seats=held_count,
                booked_seats=booked_count,
            )
        )

    return summaries


@app.get("/api/events/{event_id}/seats", response_model=List[SeatOut], tags=["Seats"])
async def get_event_seats(event_id: int, db: AsyncSession = Depends(get_db)):
    """
    Returns real-time seat map with Redis hold overlays.
    Computes effective state:
      - BOOKED: Confirmed in database
      - HELD: Locked in Redis with countdown timer (TTL)
      - AVAILABLE: Open for grab
    """
    # Verify event exists
    event_res = await db.execute(select(Event).where(Event.id == event_id))
    event = event_res.scalar_one_or_none()
    if not event:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Event not found.")

    # Fetch all seats
    seats_res = await db.execute(select(Seat).where(Seat.event_id == event_id).order_by(Seat.id))
    seats = seats_res.scalars().all()

    # Fetch active Redis holds
    active_holds = await redis_client.get_all_active_holds_for_event(event_id)

    response_seats = []
    for seat in seats:
        if seat.status == "BOOKED":
            effective_status = "BOOKED"
            holder = None
            ttl = None
        elif seat.id in active_holds:
            effective_status = "HELD"
            holder = active_holds[seat.id]["user_id"]
            ttl = active_holds[seat.id]["ttl_remaining"]
        else:
            effective_status = "AVAILABLE"
            holder = None
            ttl = None

        response_seats.append(
            SeatOut(
                id=seat.id,
                seat_number=seat.seat_number,
                row=seat.row,
                tier=seat.tier,
                price=seat.price,
                status=effective_status,
                held_by=holder,
                ttl_remaining=ttl,
            )
        )

    return response_seats


@app.post("/api/seats/hold", response_model=HoldSeatResponse, tags=["Flash Ticketing"])
async def hold_seat(
    payload: HoldSeatRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """
    High-concurrency seat hold endpoint.
    Locks the seat for checkout for 10 minutes (600s) using atomic Redis Distributed Lock.
    Prevents race conditions when thousands of users click the same seat simultaneously.
    """
    # 1. Anti-scalping rate limiting (30 requests/min per client IP)
    client_ip = request.client.host if request.client else payload.user_id
    allowed, _ = await redis_client.check_rate_limit(client_ip, limit=30, window_seconds=60)
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded. Please slow down your requests.",
        )

    # 2. Verify seat in DB
    seat_query = await db.execute(
        select(Seat).where(Seat.id == payload.seat_id, Seat.event_id == payload.event_id)
    )
    seat = seat_query.scalar_one_or_none()
    if not seat:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Seat not found.")

    if seat.status == "BOOKED":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Seat {seat.seat_number} is already permanently booked.",
        )

    # 3. Attempt atomic lock in Redis
    success, message, ttl = await redis_client.acquire_seat_hold(
        event_id=payload.event_id,
        seat_id=payload.seat_id,
        user_id=payload.user_id,
    )

    if not success:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=message)

    return HoldSeatResponse(
        success=True,
        message=message,
        event_id=payload.event_id,
        seat_id=payload.seat_id,
        held_by=payload.user_id,
        ttl_seconds=ttl,
    )


@app.post("/api/seats/confirm", response_model=ConfirmBookingResponse, tags=["Flash Ticketing"])
async def confirm_booking(
    payload: ConfirmBookingRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Confirms booking and processes ticket checkout.
    1. Verifies customer holds the active Redis lock.
    2. Executes ACID transaction to update seat status to BOOKED and insert Booking record.
    3. Releases Redis hold key upon completion.
    """
    # 1. Verify hold in Redis
    hold_info = await redis_client.get_seat_hold_info(payload.event_id, payload.seat_id)
    if not hold_info:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Seat hold has expired or was not acquired. Please hold the seat first.",
        )

    if hold_info["user_id"] != payload.user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This seat reservation belongs to another customer session.",
        )

    # 2. Fetch event and seat from DB
    event_query = await db.execute(select(Event).where(Event.id == payload.event_id))
    event = event_query.scalar_one_or_none()
    if not event:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Event not found.")

    seat_query = await db.execute(
        select(Seat).where(Seat.id == payload.seat_id, Seat.event_id == payload.event_id)
    )
    seat = seat_query.scalar_one_or_none()
    if not seat:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Seat not found.")

    if seat.status == "BOOKED":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Seat has already been booked by another transaction.",
        )

    # 3. Perform atomic DB write with optimistic locking check
    booking_ref = f"FS-{uuid.uuid4().hex[:8].upper()}"
    new_booking = Booking(
        booking_reference=booking_ref,
        event_id=payload.event_id,
        seat_id=payload.seat_id,
        user_id=payload.user_id,
        amount_paid=seat.price,
        status="CONFIRMED",
    )

    # Update seat status and increment version counter
    seat.status = "BOOKED"
    seat.version += 1

    db.add(new_booking)
    await db.commit()
    await db.refresh(new_booking)

    # 4. Release Redis hold lock
    await redis_client.release_seat_hold(payload.event_id, payload.seat_id, payload.user_id)

    return ConfirmBookingResponse(
        success=True,
        message="Ticket purchase successfully confirmed! Enjoy the event.",
        booking_reference=booking_ref,
        event_name=event.name,
        seat_number=seat.seat_number,
        tier=seat.tier,
        amount_paid=seat.price,
        user_id=payload.user_id,
        confirmed_at=new_booking.created_at,
    )


@app.post("/api/seats/release", tags=["Flash Ticketing"])
async def release_seat(
    payload: ReleaseSeatRequest,
):
    """
    Customer abandons checkout or deselects seat.
    Releases Redis lock immediately, returning the seat to the public available pool.
    """
    released = await redis_client.release_seat_hold(
        event_id=payload.event_id,
        seat_id=payload.seat_id,
        user_id=payload.user_id,
    )

    if not released:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unable to release lock: lock expired or not owned by user.",
        )

    return {"success": True, "message": "Seat hold released successfully."}


@app.get("/api/bookings/{booking_ref}", tags=["Bookings"])
async def get_booking_details(booking_ref: str, db: AsyncSession = Depends(get_db)):
    """Retrieves full details for a confirmed booking reference."""
    query = await db.execute(
        select(Booking, Seat, Event)
        .join(Seat, Booking.seat_id == Seat.id)
        .join(Event, Booking.event_id == Event.id)
        .where(Booking.booking_reference == booking_ref)
    )
    result = query.first()

    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Booking reference not found.")

    booking, seat, event = result
    return {
        "booking_reference": booking.booking_reference,
        "event_id": event.id,
        "event_name": event.name,
        "venue": event.venue,
        "event_time": event.event_time,
        "seat_number": seat.seat_number,
        "row": seat.row,
        "tier": seat.tier,
        "amount_paid": booking.amount_paid,
        "user_id": booking.user_id,
        "status": booking.status,
        "created_at": booking.created_at,
    }


@app.post("/api/admin/reset-demo", response_model=ResetResponse, tags=["Admin / Demo"])
async def reset_demo(db: AsyncSession = Depends(get_db)):
    """
    Resets all seats back to AVAILABLE and flushes all Redis holds.
    Ideal for testing flash sale bursts repeatedly!
    """
    # 1. Flush Redis holds
    await redis_client.clear_all_holds()

    # 2. Clear bookings and reset DB seats to AVAILABLE
    await db.execute(delete(Booking))
    await db.execute(update(Seat).values(status="AVAILABLE", version=1))
    await db.commit()

    # Count available
    count_query = await db.execute(select(Seat).where(Seat.status == "AVAILABLE"))
    avail_count = len(count_query.scalars().all())

    return ResetResponse(
        message="FlashSeat demo successfully reset. All seats are now AVAILABLE.",
        available_seats_count=avail_count,
    )
