import os
import random
import time
import redis
from faker import Faker
from dotenv import load_dotenv

# ---- Load .env ----
load_dotenv()

# ---- Config ----
#REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_URL = "rediss://default:Oqu2qhjEVr9EOXqEBf3mzWbq78JQNsZD@redis-19585.c44444.us-east-1-mz.ec2.cloud.rlrcp.com:19585"
TOTAL_SESSIONS = int(os.getenv("TOTAL_SESSIONS", 1000))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
KEY_PREFIX = os.getenv("KEY_PREFIX", "session:")

# TTL window in seconds (min/max). Each session gets a random TTL within this range.
TTL_MIN_SECONDS = int(os.getenv("TTL_MIN_SECONDS", 30 * 60))        # 30m
TTL_MAX_SECONDS = int(os.getenv("TTL_MAX_SECONDS", 7 * 24 * 3600))  # 7d

# ---- Data generators (Brazil-focused) ----
fake = Faker("pt_BR")

DEVICE_TYPES = ["desktop", "mobile", "tablet"]
OS_BUCKET = ["Windows", "macOS", "Linux", "Android", "iOS"]
BROWSER_BUCKET = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]

BR_CITIES = [
    ("São Paulo",    (-46.6333, -23.5505)),
    ("Rio de Janeiro", (-43.1964, -22.9083)),
    ("Belo Horizonte", (-43.9378, -19.9208)),
    ("Curitiba",     (-49.2731, -25.4278)),
    ("Porto Alegre", (-51.2300, -30.0331)),
    ("Recife",       (-34.8781, -8.0539)),
    ("Salvador",     (-38.5014, -12.9777)),
    ("Fortaleza",    (-38.5247, -3.7319)),
    ("Brasília",     (-47.8825, -15.7942)),
    ("Manaus",       (-60.0250, -3.1190)),
]

ROLE_BUCKET = ["user", "admin", "support", "auditor", "ops", "partner"]
SCOPE_BUCKET = [
    "read:orders", "write:orders",
    "read:payments", "write:payments",
    "read:users", "write:users",
    "read:catalog", "write:catalog"
]

UA_FALLBACK = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 Chrome/123.0 Mobile Safari/537.36",
]

def _safe_user_agent() -> str:
    try:
        return fake.user_agent()
    except Exception:
        return random.choice(UA_FALLBACK)

def _pick_roles() -> str:
    base = ["user"]
    if random.random() < 0.15:
        base.append(random.choice([r for r in ROLE_BUCKET if r != "user"]))
    return ",".join(sorted(set(base)))

def _pick_scopes() -> str:
    n = random.randint(1, 4)
    return ",".join(sorted(set(random.sample(SCOPE_BUCKET, n))))

def _device_mix():
    device = random.choice(DEVICE_TYPES)
    if device == "desktop":
        os_name = random.choice(["Windows", "macOS", "Linux"])
        is_mobile = "0"
    else:
        os_name = random.choice(["Android", "iOS"])
        is_mobile = "1"
    browser = random.choice(BROWSER_BUCKET)
    return device, os_name, browser, is_mobile

def gen_session(i: int):
    now = int(time.time())
    ttl = random.randint(TTL_MIN_SECONDS, TTL_MAX_SECONDS)
    expires_at = now + ttl

    uid = random.randint(1, 500_000)
    email = fake.free_email()
    device, os_name, browser, is_mobile = _device_mix()
    city, (lon, lat) = random.choice(BR_CITIES)

    session = {
        # identity
        "sid": f"sess-{i:08d}",
        "uid": str(uid),
        "email": email,

        # times (unix seconds, stored as strings)
        "issued_at": str(now),
        "last_seen_at": str(now - random.randint(0, min(ttl, 3600))),  # up to 1h ago
        "expires_at": str(expires_at),

        # network / geo
        "ip": fake.ipv4_public(),
        "country": "BR",
        "city": city,
        "geo": f"{lon},{lat}",

        # device
        "user_agent": _safe_user_agent(),
        "device": device,
        "os": os_name,
        "browser": browser,
        "is_mobile": is_mobile,

        # auth/authorization flags
        "roles": _pick_roles(),
        "scopes": _pick_scopes(),
        "is_mfa": "1" if random.random() < 0.35 else "0",
        "is_sso": "1" if random.random() < 0.25 else "0",
        "revoked": "0",
    }
    key = f"{KEY_PREFIX}{session['sid']}"
    return key, session, ttl

def load_data(r: redis.Redis, total: int, batch: int):
    for start in range(0, total, batch):
        pipe = r.pipeline(transaction=False)
        end = min(start + batch, total)
        for i in range(start + 1, end + 1):
            key, session, ttl = gen_session(i)
            pipe.hset(key, mapping=session)
            pipe.expire(key, ttl)
        pipe.execute()
        if (end % (batch * 10)) == 0 or end == total:
            print(f"Inserted {end} / {total}")

def main():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    print(f"Connected to Redis: {REDIS_URL}")
    load_data(r, TOTAL_SESSIONS, BATCH_SIZE)
    print("Done.")

if __name__ == "__main__":
    main()