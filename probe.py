# probe.py
import os
import json
import time
import statistics
import redis
from dotenv import load_dotenv
from redis.exceptions import ResponseError

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
INDEX_NAME = os.getenv("INDEX_NAME", "idx:merchant")
KEY_PREFIX = os.getenv("KEY_PREFIX", "merchant:")
COUNTER_KEY = os.getenv("COUNTER_KEY", "merchant:id")  # shared with loader.py
ITERATIONS = int(os.getenv("ITERATIONS", "1000"))
SLEEP_MS = int(os.getenv("SLEEP_MS", "5"))             # poll sleep
TIMEOUT_MS = int(os.getenv("TIMEOUT_MS", "10000"))     # max wait per doc
REPORT_EVERY = int(os.getenv("REPORT_EVERY", "50"))

def server_time_ms(r) -> int:
    sec, usec = r.execute_command("TIME")
    return int(sec) * 1000 + int(usec) // 1000

def ft_info_dict(r, index):
    arr = r.execute_command("FT.INFO", index)
    it = iter(arr)
    out = {}
    for k, v in zip(it, it):
        out[k if isinstance(k, str) else k.decode()] = v
    return out

def index_exists(r, name: str) -> bool:
    try:
        r.execute_command("FT.INFO", name)
        return True
    except ResponseError:
        return False

def ensure_index(r, name: str):
    if index_exists(r, name):
        print(f"[probe] Index '{name}' exists.")
        return
    print(f"[probe] Creating index '{name}'...")
    r.execute_command(
        "FT.CREATE", name,
        "ON", "JSON",
        "PREFIX", "1", KEY_PREFIX,
        "SCHEMA",
        "$.id", "AS", "id", "NUMERIC",
        "$.cnpj", "AS", "cnpj", "TAG",
        "$.company_name", "AS", "company_name", "TEXT",
        "$.state", "AS", "state", "TAG",
        "$.channel", "AS", "channel", "TAG",
        "$.network_code", "AS", "network_code", "NUMERIC",
        "$.mcc", "AS", "mcc", "TAG",
        "$.t0ms", "AS", "t0ms", "NUMERIC"
    )

def gen_min_doc(i: int) -> dict:
    # minimal fields needed for index + a little variety
    return {
        "id": i,
        "cnpj": f"{i:014d}",
        "company_name": f"Empresa {i}",
        "state": "SP" if i % 2 == 0 else "RJ",
        "channel": "online" if i % 3 == 0 else "retail",
        "network_code": (i % 10000) + 1,
        "mcc": "5411" if i % 5 == 0 else "5812",
    }

def wait_visible(r, index: str, doc_id: int, timeout_ms: int, sleep_ms: int) -> bool:
    q = f"@id:[{doc_id} {doc_id}]"
    deadline = server_time_ms(r) + timeout_ms
    while server_time_ms(r) < deadline:
        res = r.execute_command("FT.SEARCH", index, q, "LIMIT", 0, 1)
        total = res[0] if isinstance(res, list) else int(res)
        if total >= 1:
            return True
        time.sleep(sleep_ms / 1000.0)
    return False

def main():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    print(f"[probe] Redis: {REDIS_URL}")
    ensure_index(r, INDEX_NAME)

    samples = []
    failures = 0

    for n in range(1, ITERATIONS + 1):
        # get a unique id and set t0ms from server time
        doc_id = r.incr(COUNTER_KEY)
        key = f"{KEY_PREFIX}{doc_id}"
        t0 = server_time_ms(r)
        doc = gen_min_doc(doc_id)
        doc["t0ms"] = t0
        r.execute_command("JSON.SET", key, "$", json.dumps(doc, ensure_ascii=False))

        # measure time-to-visible
        visible = wait_visible(r, INDEX_NAME, doc_id, TIMEOUT_MS, SLEEP_MS)
        t1 = server_time_ms(r)
        if visible:
            latency = t1 - t0
            samples.append(latency)
        else:
            failures += 1

        if n % REPORT_EVERY == 0 or n == ITERATIONS:
            if samples:
                p50 = statistics.median(samples)
                avg = sum(samples) / len(samples)
                print(f"[probe] n={len(samples)} p50={p50:.2f}ms avg={avg:.2f}ms fail={failures}")
            else:
                print(f"[probe] n=0 fail={failures}")

            # optional FT.INFO snapshot
            try:
                info = ft_info_dict(r, INDEX_NAME)
                pct = float(info.get("percent_indexed", 0))
                ndocs = int(info.get("num_docs", 0))
                mem_mb = float(info.get("total_index_memory_sz_mb", 0.0))
                print(f"[probe] FT.INFO percent_indexed={pct:.2f} num_docs={ndocs:,} mem={mem_mb:.3f}MB")
            except Exception as e:
                print(f"[probe] FT.INFO error: {e}")

    if samples:
        p50 = statistics.median(samples)
        avg = sum(samples) / len(samples)
        print(f"[probe] FINAL n={len(samples)} p50={p50:.2f}ms avg={avg:.2f}ms fail={failures}")
    else:
        print(f"[probe] FINAL n=0 fail={failures}")

if __name__ == "__main__":
    main()