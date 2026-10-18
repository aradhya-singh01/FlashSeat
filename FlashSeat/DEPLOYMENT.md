# 🚀 FlashSeat Cloud Deployment Guide

This guide walks you through deploying **FlashSeat** to **Render** or **Railway** in under 3 minutes.

---

## ⚡ Option 1: Deploy on Render

### Step 1: Push Code to GitHub
Ensure your repository contains:
- `requirements.txt`
- `Procfile` and/or `render.yaml`
- `app/` folder with `main.py`, `database.py`, `redis_client.py`, and `static/index.html`

### Step 2: Create a Web Service on Render
1. Log in to [Render Dashboard](https://dashboard.render.com).
2. Click **New +** > **Web Service**.
3. Connect your GitHub repository containing FlashSeat.
4. Configure the service settings:
   - **Name**: `flashseat`
   - **Region**: Choose the closest region (e.g., Oregon / Frankfurt / Singapore)
   - **Runtime**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt && python -m app.seed`
   - **Start Command**: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
   - **Instance Type**: `Free`

### Step 3: (Optional) Add Free Managed Redis on Render
1. In the Render Dashboard, click **New +** > **Redis**.
2. Name it `flashseat-redis` and select the Free plan.
3. In your Web Service settings, add an Environment Variable:
   - `REDIS_URL`: `<Internal Redis Connection URL>` from your Redis instance.
*(Note: If you do not add Redis, FlashSeat automatically activates its built-in in-memory fallback with identical 10-minute hold TTL expiration).*

---

## 🚂 Option 2: Deploy on Railway

### Step 1: Deploy from GitHub Repo
1. Log in to [Railway Dashboard](https://railway.app).
2. Click **New Project** > **Deploy from GitHub repo**.
3. Select your FlashSeat repository.

Railway will automatically detect `railway.json` and `Procfile`, install all dependencies from `requirements.txt`, run the database seed, and start the Uvicorn server on `$PORT`.

### Step 2: (Optional) Add Redis Plugin on Railway
1. Inside your Railway project canvas, click **+ New** > **Database** > **Add Redis**.
2. Railway automatically sets the `REDIS_URL` environment variable for your web service!

---

## 🐳 Option 3: Deploy via Docker (Any Platform)

FlashSeat includes a production `Dockerfile`:
```bash
# Build the image
docker build -t flashseat .

# Run the container
docker run -p 8000:8000 -e PORT=8000 flashseat
```

---

## 📋 Deployed Endpoints Checklist

Once deployed, visit your live URL (e.g. `https://flashseat.onrender.com` or `https://flashseat.up.railway.app`):
- `GET /` — Interactive Dark-Mode Seat Matrix & Booking Frontend
- `GET /api/health` — Engine Status & Distributed Lock Diagnostics
- `GET /docs` — Interactive OpenAPI / Swagger Documentation
- `GET /api/events` — Live events list with availability counts
