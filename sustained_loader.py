# loader.py
import os
import json
import time
import random
import redis
from faker import Faker
from dotenv import load_dotenv

load_dotenv()

# ---- Config ----
#REDIS_URL = os.getenv("REDIS_URL", "rediss://default:Oqu2qhjEVr9EOXqEBf3mzWbq78JQNsZD@redis-19585.c44444.us-east-1-mz.ec2.cloud.rlrcp.com:19585")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
KEY_PREFIX = os.getenv("KEY_PREFIX", "merchant:")
COUNTER_KEY = os.getenv("COUNTER_KEY", "merchant:id")  # shared with probe.py
TARGET_RPS = float(os.getenv("TARGET_RPS", "2500"))    # aim for ~2k ops/sec
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))       # pipeline size
DURATION_S = int(os.getenv("DURATION_S", "300"))       # run time; set 0 to run forever

# ---- Fake data (Brazil) ----
fake = Faker("pt_BR")
channels = ["online", "retail", "hybrid", "food_delivery", "b2b", "services", "luxury"]
mcc_codes = [
    "5732", "5411", "5734", "5814", "5691", "1520", "5912", "5533",
    "5942", "5462", "7538", "8021", "5995", "7997", "5812", "5094",
    "8398", "5992"
]

def gen_doc(i: int) -> dict:
    company_name = fake.company()
    return {
        "id": i,
        "cnpj": fake.cnpj(),
        "company_name": company_name,
        "company_name_short": company_name.split(" ")[0][:10],
        "street": fake.street_name(),
        "number": str(fake.building_number()),
        "neighborhood": fake.bairro(),
        "city": fake.city(),
        "state": fake.estado_sigla(),
        "zip_code": fake.postcode(),
        "channel": random.choice(channels),
        "email": f"contato@{company_name.lower().replace(' ', '').replace(',', '')}.com.br",
        "phone": fake.phone_number(),
        "network_code": random.randint(1, 9999),
        "mcc": random.choice(mcc_codes),
    }

def main():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    print(f"[loader] Redis: {REDIS_URL}")
    print(f"[loader] Target ~{TARGET_RPS:.0f} JSON.SET ops/sec, batch={BATCH_SIZE}, duration={DURATION_S}s")

    # pacing
    per_op = 1.0 / max(TARGET_RPS, 0.001)
    per_batch_budget = per_op * BATCH_SIZE

    start = time.time()
    sent = 0
    loops = 0

    while True:
        t_loop = time.perf_counter()

        # Reserve a contiguous id range atomically
        hi = r.incrby(COUNTER_KEY, BATCH_SIZE)
        lo = hi - BATCH_SIZE + 1

        pipe = r.pipeline(transaction=False)
        for i in range(lo, hi + 1):
            key = f"{KEY_PREFIX}{i}"
            pipe.execute_command("JSON.SET", key, "$", json.dumps(gen_doc(i), ensure_ascii=False))
        pipe.execute()
        sent += BATCH_SIZE
        loops += 1

        # simple pacing to hit approximate RPS target
        elapsed = time.perf_counter() - t_loop
        sleep_for = per_batch_budget - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)

        if loops % 20 == 0:
            run_s = time.time() - start
            rps = sent / max(run_s, 1e-6)
            print(f"[loader] sent={sent:,} rps≈{rps:,.0f}")

        if DURATION_S > 0 and (time.time() - start) >= DURATION_S:
            break

    run_s = time.time() - start
    rps = sent / max(run_s, 1e-6)
    print(f"[loader] Done. sent={sent:,} over {run_s:.1f}s (rps≈{rps:,.0f})")

if __name__ == "__main__":
    main()