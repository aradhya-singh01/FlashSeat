"""
FlashSeat Database Seeder Script
Populates the database with realistic events and tiered seat layouts for flash sale testing.

Can be run via:
    python app/seed.py
    python -m app.seed
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List

# Ensure project root is in sys.path regardless of how script is invoked
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Safe encoding on Windows consoles
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from sqlalchemy import delete, select, func
from app.database import Base, engine, AsyncSessionLocal, Event, Seat, Booking

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("flashseat.seeder")

# Event layout templates with tiered pricing and seat counts
EVENTS_SEED_DATA = [
    {
        "name": "Taylor Swift | The Eras Tour (Encore Flash Sale)",
        "description": "Exclusive flash seat reservation for the final worldwide stadium shows.",
        "venue": "SoFi Stadium, Los Angeles, CA",
        "days_from_now": 30,
        "rows_config": [
            {"row": "A", "count": 12, "tier": "VIP Diamond Floor", "price": 499.0},
            {"row": "B", "count": 12, "tier": "VIP Gold Floor", "price": 389.0},
            {"row": "C", "count": 16, "tier": "Club Premium", "price": 249.0},
            {"row": "D", "count": 16, "tier": "Club Lower", "price": 189.0},
            {"row": "E", "count": 20, "tier": "Upper Bowl", "price": 119.0},
        ],
        "sample_booked_seats": ["A01", "A02"],
    },
    {
        "name": "NBA Finals 2026 - Game 7 Flash Ticketing",
        "description": "High-stakes championship decider with courtside and lower bowl rush tickets.",
        "venue": "Madison Square Garden, New York, NY",
        "days_from_now": 45,
        "rows_config": [
            {"row": "A", "count": 10, "tier": "Courtside Row 1", "price": 850.0},
            {"row": "B", "count": 12, "tier": "Lower Bowl Prime", "price": 320.0},
            {"row": "C", "count": 14, "tier": "Lower Bowl Corner", "price": 210.0},
            {"row": "D", "count": 16, "tier": "Mezzanine Center", "price": 140.0},
            {"row": "E", "count": 18, "tier": "Balcony General", "price": 85.0},
        ],
        "sample_booked_seats": ["A01"],
    },
    {
        "name": "Hans Zimmer Live: Symphonic Cinema Experience",
        "description": "Orchestral live performances of Inception, Interstellar, Dune, and The Dark Knight.",
        "venue": "Royal Albert Hall, London, UK",
        "days_from_now": 60,
        "rows_config": [
            {"row": "A", "count": 8, "tier": "Royal Box VIP", "price": 350.0},
            {"row": "B", "count": 14, "tier": "Arena Stalls Prime", "price": 220.0},
            {"row": "C", "count": 16, "tier": "Grand Tier Center", "price": 160.0},
            {"row": "D", "count": 18, "tier": "Circle Reserved", "price": 95.0},
        ],
        "sample_booked_seats": ["A01"],
    },
]


async def seed_database(reset_existing: bool = True):
    """
    Clears existing bookings/seats/events (if reset_existing=True)
    and seeds fresh events, seats, and sample bookings.
    """
    logger.info("Initializing database schema...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSessionLocal() as session:
        if reset_existing:
            logger.info("Clearing previous demo records...")
            await session.execute(delete(Booking))
            await session.execute(delete(Seat))
            await session.execute(delete(Event))
            await session.commit()

        total_events_created = 0
        total_seats_created = 0
        total_bookings_created = 0

        now = datetime.now(timezone.utc)

        for event_data in EVENTS_SEED_DATA:
            event_time = now + timedelta(days=event_data["days_from_now"])
            rows_config = event_data["rows_config"]
            total_seats_for_event = sum(rc["count"] for rc in rows_config)
            sample_booked_set = set(event_data.get("sample_booked_seats", []))

            event = Event(
                name=event_data["name"],
                description=event_data["description"],
                venue=event_data["venue"],
                event_time=event_time,
                total_seats=total_seats_for_event,
            )
            session.add(event)
            await session.flush()  # Populates event.id

            seats_to_insert: List[Seat] = []
            for rc in rows_config:
                row_letter = rc["row"]
                for seat_idx in range(1, rc["count"] + 1):
                    seat_num = f"{row_letter}{seat_idx:02d}"
                    is_booked = seat_num in sample_booked_set
                    seat = Seat(
                        event_id=event.id,
                        seat_number=seat_num,
                        row=row_letter,
                        tier=rc["tier"],
                        price=rc["price"],
                        status="BOOKED" if is_booked else "AVAILABLE",
                        version=2 if is_booked else 1,
                    )
                    seats_to_insert.append(seat)

            session.add_all(seats_to_insert)
            await session.flush()

            # Create sample booking records for the booked seats
            for seat in seats_to_insert:
                if seat.status == "BOOKED":
                    ref_code = f"FS-{event.id}{seat.seat_number}-DEMO"
                    booking = Booking(
                        booking_reference=ref_code,
                        event_id=event.id,
                        seat_id=seat.id,
                        user_id=f"user_demo_{seat.seat_number.lower()}",
                        amount_paid=seat.price,
                        status="CONFIRMED",
                    )
                    session.add(booking)
                    total_bookings_created += 1

            total_events_created += 1
            total_seats_created += len(seats_to_insert)

            logger.info(
                "Created event '%s' (ID: %d) with %d tiered seats (%d pre-booked).",
                event.name,
                event.id,
                len(seats_to_insert),
                len(sample_booked_set),
            )

        await session.commit()

        # Summary statistics
        events_count = (await session.execute(select(func.count(Event.id)))).scalar_one()
        seats_count = (await session.execute(select(func.count(Seat.id)))).scalar_one()
        bookings_count = (await session.execute(select(func.count(Booking.id)))).scalar_one()
        available_seats = (await session.execute(select(func.count(Seat.id)).where(Seat.status == "AVAILABLE"))).scalar_one()
        booked_seats = (await session.execute(select(func.count(Seat.id)).where(Seat.status == "BOOKED"))).scalar_one()

        print("\n" + "=" * 60)
        print("[SUCCESS] SEEDING COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"  * Total Events:     {events_count}")
        print(f"  * Total Seats:      {seats_count}")
        print(f"  * Available Seats:  {available_seats}")
        print(f"  * Booked Seats:     {booked_seats}")
        print(f"  * Sample Bookings:  {bookings_count}")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(seed_database(reset_existing=True))
