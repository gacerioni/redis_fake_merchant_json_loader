# Merchant Data Loader & Probe for Redis

This repo contains scripts to **inject sustained JSON load** into Redis and **probe time-to-index** under load.

## Components

- **`sustained_loader.py`** — Continuously `JSON.SET` fake Brazilian merchant docs at a target RPS.
- **`probe.py`** — Inserts a single doc, immediately searches for it, and measures *time-to-index*. Can print `FT.INFO` stats.
- **`main.py`** — Legacy bulk loader, optional use.

## Features

- Generates realistic Brazilian merchant data (`faker` with `pt_BR` locale).
- Uses RedisJSON & RediSearch.
- Shared atomic counter prevents key collisions between loader and probe.
- Index auto-creation (if missing) with minimal schema.
- `.env`-driven config.

## Requirements

- Python 3.8+
- Redis with RedisJSON & RediSearch modules.

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate   # Linux / macOS
venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

### Example `requirements.txt`
```
redis
faker
python-dotenv
```

## Configuration

Example `.env`:

```env
# Redis
REDIS_URL=redis://default:<password>@<host>:<port>

# Shared ID space for loader+probe
COUNTER_KEY=merchant:id
KEY_PREFIX=merchant:
INDEX_NAME=idx:merchant

# Loader pacing
TARGET_RPS=2000
BATCH_SIZE=200
DURATION_S=0         # 0 = run until Ctrl+C

# Probe cadence
ITERATIONS=1000
SLEEP_MS=5
TIMEOUT_MS=10000
REPORT_EVERY=50
```

> **Tip:** `TARGET_RPS=2000` with `BATCH_SIZE=200` is a safe start; adjust if Redis has headroom.

## Usage

### Terminal A — sustained injector
```bash
source .venv/bin/activate
export $(grep -v '^#' .env | xargs)
python sustained_loader.py
```
Output:
```
[loader] Redis: redis://...
[loader] Target ~2000 JSON.SET ops/sec, batch=200, duration=0s
[loader] sent=20,000 rps≈1,980
...
```

Watch Redis ops/sec and latency in Grafana or `redis-cli MONITOR`.

### Terminal B — probe time-to-index
```bash
source .venv/bin/activate
export $(grep -v '^#' .env | xargs)
python probe.py
```
Output:
```
[probe] Redis: redis://...
[probe] Index 'idx:merchant' exists.
[probe] n=50 p50=4.10ms avg=4.65ms fail=0
[probe] FT.INFO percent_indexed=1.00 num_docs=200,750 mem=45.312MB
...
[probe] FINAL n=1000 p50=4.05ms avg=4.58ms fail=0
```

**Interpretation:**
- `p50`/`avg` = time for a new JSON to be searchable.
- `fail` > 0 means `TIMEOUT_MS` too low or Redis overloaded.
- `FT.INFO` shows doc count and index memory.

## Notes
- Loader + probe share a key counter to avoid collisions.
- Probe can be extended to also run search performance benchmarks.
