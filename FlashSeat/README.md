# ⚡ FlashSeat: High-Concurrency Flash Ticket Booking Engine

FlashSeat is a high-performance flash sale ticketing and seat reservation backend engine built with **FastAPI**, **Redis**, and **SQLAlchemy**.

---

## 🌟 Key Features

1. **Distributed Seat Holds (TTL Locks)**:
   - When a user selects a seat during a flash sale, an atomic Redis lock (`SET ... NX EX 600`) holds the seat for 10 minutes.
   - Prevents double-booking even under massive concurrent traffic spikes.

2. **Atomic Lock Release**:
   - Uses Redis Lua scripts to guarantee that only the user who holds the lock can release it or check out.

3. **Optimistic Locking & ACID Persistence**:
   - SQLAlchemy with `version` counter ensures safe database state transitions (`AVAILABLE` -> `HELD` -> `BOOKED`).
   - Supports SQLite (`aiosqlite`) out of the box for instant zero-setup development, and PostgreSQL (`asyncpg`) for production.

4. **Zero-Crash Resilient Fallback**:
   - If Redis is temporarily offline, the engine activates a thread-safe async in-memory fallback manager with identical TTL expiration semantics.

5. **Anti-Scalping Rate Limiting**:
   - Protects seat checkout endpoints against rapid bot scalping.

---

## 🚀 Quickstart

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Start the Server
```bash
uvicorn app.main:app --reload --port 8000
```

### 3. Open Interactive API Docs
Visit: [http://localhost:8000/docs](http://localhost:8000/docs)

---

## 📋 Core API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | FlashSeat Single-Page Interactive Booking Frontend |
| `GET` | `/api/health` | Engine health check & Redis connection status |
| `GET` | `/api/events` | List events with real-time seat availability counts |
| `GET` | `/api/events/{event_id}/seats` | Real-time seat map (AVAILABLE vs HELD vs BOOKED) |
| `POST` | `/api/seats/hold` | Atomically reserve a seat for 10 minutes during checkout |
| `POST` | `/api/seats/confirm` | Finalize ticket purchase & persist booking record |
| `POST` | `/api/seats/release` | Abandon checkout and release seat hold immediately |
| `GET` | `/api/bookings/{booking_ref}`| View confirmed booking details |
| `POST` | `/api/admin/reset-demo` | Reset all seats to AVAILABLE for testing |

---

## ☁️ Cloud Deployment (Render & Railway)

FlashSeat is fully pre-configured for instant zero-config cloud deployment:

- **Render**: Connect your GitHub repository. Render automatically reads `render.yaml` or `Procfile`.
  - Build command: `pip install -r requirements.txt && python -m app.seed`
  - Start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
- **Railway**: Connect your GitHub repository. Railway automatically reads `railway.json` and deploys seamlessly.
- **Docker**: Build and run with `docker build -t flashseat .` and `docker run -p 8000:8000 flashseat`.

See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed step-by-step instructions.
