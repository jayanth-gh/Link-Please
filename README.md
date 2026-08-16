# LinkPlease Minimal Backend

Run a minimal Flask app that implements the required endpoints for the assignment.

Requirements:
- Python 3.10+
- Set environment variable `PSEUDOGRAM_API_KEY` with your API key.

Install and run:

```bash
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
PSEUDOGRAM_API_KEY=your_key python app.py
```

Endpoints:
- `POST /rules` — create rule {"keyword":"PRICE","dm_message":"..."}
- `POST /webhook` — receives events (returns 200 quickly)
- `GET /stats` — returns sent/failed/queued/duplicates_blocked

This is intentionally minimal and uses an on-disk `data.db` SQLite file for persistence.
