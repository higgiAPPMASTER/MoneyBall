# ⚾ MLB Daily Picks — Web App

A private MLB batter analysis tool that runs your 4-step picks algorithm and displays the Top 9 plays for any date.

## How the algorithm works

| Step | Source | Filter |
|------|--------|--------|
| S1 | Fantasy Info Central | Lifetime BA vs today's pitcher ≥ .250, min 5 AB |
| S2 | StatMuse | Lifetime H/A BA vs today's opponent ≥ .250, min 3 games |
| S3 | StatMuse | 2026 season H/A BA vs all teams ≥ .250, min 3 games |
| S4 | ESPN splits | 2026 Day/Night BA ≥ .200 (filter only, no score impact) |

**Score** = (S1 × 1000) + (S2 × 1000) + (S3 × 1000). Higher is better.

---

## Deploy to Render.com (one-time setup, ~10 minutes)

### 1. Push to GitHub
```bash
cd mlb-picks-app
git init
git add .
git commit -m "MLB Daily Picks app"
# Create a repo on github.com, then:
git remote add origin https://github.com/YOUR_USERNAME/mlb-picks-app.git
git push -u origin main
```

### 2. Create Render Web Service
1. Go to [render.com](https://render.com) → **New → Web Service**
2. Connect your GitHub repo
3. Render auto-detects `render.yaml` — click **Deploy**

### 3. Set environment variables on Render
In the Render dashboard → **Environment**:

| Variable | Value |
|----------|-------|
| `SECRET_KEY` | Any long random string (click "Generate") |
| `USERS` | `yourname:yourpassword,friend1:theirpassword` |

### 4. That's it! 🎉
Your app is live at `https://mlb-daily-picks-XXXX.onrender.com`

---

## Run locally (for testing)

```bash
# Install dependencies
pip install -r requirements.txt

# Create your .env (copy the example)
cp .env.example .env
# Edit .env with your own SECRET_KEY and USERS

# Start the server
uvicorn main:app --reload --port 8000

# Open http://localhost:8000
```

---

## Add / remove friends

Edit the `USERS` environment variable on Render:
```
USERS="you:yourpass,alice:alice_secret,bob:bob_secret"
```
No restart needed on Render — just save and it redeploys.

---

## Pricing

| Tier | Cost | Notes |
|------|------|-------|
| Free | $0/mo | Sleeps after 15 min idle, ~30s cold start |
| Starter | $7/mo | Always-on, no cold starts ← **Recommended** |

---

## Tech stack

- **Backend**: FastAPI + Python 3.11
- **Streaming**: Server-Sent Events (SSE) for real-time progress
- **Auth**: JWT (24-hour tokens), users in env var
- **Frontend**: Single-page HTML, Tailwind CSS, vanilla JS
- **Data sources**: Fantasy Info Central, StatMuse, ESPN (all public)
