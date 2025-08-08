# Merchant Data Loader for Redis

This script bulk-loads a large dataset of fake Brazilian merchant JSON documents into Redis,
with optional RediSearch indexing. It uses `faker` with the `pt_BR` locale to generate realistic data
(CNPJ, addresses, company names, etc.) and pipelines for efficient inserts.

## Features

- Generates realistic Brazilian merchant data
- Supports **RedisJSON** and **RediSearch**
- Automatically creates an index (if not existing) after or before the load
- Uses `.env` for configuration
- Connects to Redis locally or in Redis Cloud via a **full Redis URL**
- Tunable batch size and total number of documents

## Requirements

- Python 3.8+
- Redis with RedisJSON and RediSearch modules enabled

## Installation

Create and activate a virtual environment (recommended):

```bash
python3 -m venv venv
source venv/bin/activate   # Linux / macOS
venv\Scripts\activate    # Windows
```

Install dependencies:

```bash
pip install -r requirements.txt
```

### `requirements.txt` example

```
redis
faker
python-dotenv
```

## Configuration

Set environment variables in `.env`:

```env
# Redis connection (local by default)
REDIS_URL=redis://default:<password>@<host>:<port>

# Load config
TOTAL_DOCS=1000000
BATCH_SIZE=1000
INDEX_NAME=idx:merchant
KEY_PREFIX=merchant:
CREATE_INDEX_AFTER_LOAD=true
```

## Running

```bash
python load_merchants.py
```

## Notes

- If `CREATE_INDEX_AFTER_LOAD=true`, the index will be created after all inserts for better performance.
- If the index already exists, the script will skip creation.
- Generated fields include: id, CNPJ, company name, address, city, state, ZIP, sales channel, email, phone, network code, and MCC.
