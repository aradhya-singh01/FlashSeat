"""
FlashSeat Database Layer
SQLAlchemy Async ORM models, session factory, and database initialization.
Supports SQLite (via aiosqlite) for instant local zero-config execution,
and PostgreSQL (via asyncpg) for production.
"""

import os
from datetime import datetime, timezone
from typing import AsyncGenerator

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    DateTime,
    ForeignKey,
    Text,
    select,
    func,
)
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import declarative_base, relationship

# Database configuration with SQLite fallback
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./flashseat.db")

# Create asynchronous engine
# SQLite requires connect_args check_same_thread=False
connect_args = {"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    connect_args=connect_args,
)

# Async session factory
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)

Base = declarative_base()


class Event(Base):
    """Event model representing concerts, games, or shows."""
    __tablename__ = "events"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text, nullable=True)
    venue = Column(String(255), nullable=False)
    total_seats = Column(Integer, nullable=False, default=60)
    event_time = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    # Relationships
    seats = relationship("Seat", back_populates="event", cascade="all, delete-orphan")
    bookings = relationship("Booking", back_populates="event")


class Seat(Base):
    """
    Seat model representing individual seats for an event.
    Status values:
      - 'AVAILABLE': Open for booking
      - 'HELD': Temporarily locked by a user during checkout (tracked in Redis with TTL)
      - 'BOOKED': Permanently confirmed and paid
    """
    __tablename__ = "seats"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    event_id = Column(Integer, ForeignKey("events.id"), nullable=False, index=True)
    seat_number = Column(String(20), nullable=False, index=True)  # e.g., 'A1', 'B12'
    row = Column(String(10), nullable=False)                      # e.g., 'A', 'B'
    tier = Column(String(50), nullable=False, default="Standard") # 'VIP', 'Premium', 'Standard'
    price = Column(Float, nullable=False, default=150.0)
    status = Column(String(20), nullable=False, default="AVAILABLE")
    version = Column(Integer, nullable=False, default=1)          # Optimistic locking counter

    # Relationships
    event = relationship("Event", back_populates="seats")
    booking = relationship("Booking", back_populates="seat", uselist=False)


class Booking(Base):
    """Booking model for confirmed seat reservations."""
    __tablename__ = "bookings"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    booking_reference = Column(String(50), unique=True, index=True, nullable=False)
    event_id = Column(Integer, ForeignKey("events.id"), nullable=False)
    seat_id = Column(Integer, ForeignKey("seats.id"), nullable=False, unique=True)
    user_id = Column(String(100), nullable=False, index=True)
    amount_paid = Column(Float, nullable=False)
    status = Column(String(20), nullable=False, default="CONFIRMED")  # 'CONFIRMED', 'CANCELLED'
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    # Relationships
    event = relationship("Event", back_populates="bookings")
    seat = relationship("Seat", back_populates="booking")


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency yielding an async database session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db() -> None:
    """
    Initializes database tables and seeds demo data if the database is newly created.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Seed initial event & seats if database is empty
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(func.count(Event.id)))
        count = result.scalar_one()

        if count == 0:
            # Create flagship FlashSeat demo event
            demo_event = Event(
                name="Coldplay: Music of the Spheres World Tour (Flash Sale)",
                description="Live concert flash ticketing event with high-concurrency seat reservation.",
                venue="MetLife Stadium, East Rutherford",
                total_seats=60,
                event_time=datetime(2026, 11, 20, 20, 0, 0),
            )
            session.add(demo_event)
            await session.flush()  # Populates demo_event.id

            # Create 60 seats (Rows A, B, C, D with 15 seats each)
            rows = [
                ("A", "VIP Front Row", 350.0),
                ("B", "VIP Lounge", 250.0),
                ("C", "Premium General", 175.0),
                ("D", "Standard Arena", 100.0),
            ]

            seats_to_insert = []
            for row_letter, tier_name, price in rows:
                for col in range(1, 16):
                    seat_num = f"{row_letter}{col:02d}"
                    seat = Seat(
                        event_id=demo_event.id,
                        seat_number=seat_num,
                        row=row_letter,
                        tier=tier_name,
                        price=price,
                        status="AVAILABLE",
                        version=1,
                    )
                    seats_to_insert.append(seat)

            session.add_all(seats_to_insert)
            await session.commit()
