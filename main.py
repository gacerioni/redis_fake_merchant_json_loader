import json
import random
import redis
import os
from faker import Faker
from redis.exceptions import ResponseError
from dotenv import load_dotenv

# ---- Load .env ----
load_dotenv()

# ---- Config ----
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
TOTAL_DOCS = int(os.getenv("TOTAL_DOCS", 10_000_000))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
INDEX_NAME = os.getenv("INDEX_NAME", "idx:merchant")
KEY_PREFIX = os.getenv("KEY_PREFIX", "merchant:")
CREATE_INDEX_AFTER_LOAD = os.getenv("CREATE_INDEX_AFTER_LOAD", "true").lower() == "true"

# ---- Data generators (Brazilian) ----
fake = Faker("pt_BR")
channels = ["online", "retail", "hybrid", "food_delivery", "b2b", "services", "luxury"]
mcc_codes = ["5732", "5411", "5734", "5814", "5691", "1520", "5912", "5533",
             "5942", "5462", "7538", "8021", "5995", "7997", "5812", "5094",
             "8398", "5992"]

def gen_merchant(i: int) -> dict:
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

# ---- Index management ----
def index_exists(r: redis.Redis, name: str) -> bool:
    try:
        r.execute_command("FT.INFO", name)
        return True
    except ResponseError:
        return False

def ensure_index(r: redis.Redis, name: str):
    if index_exists(r, name):
        print(f"Index '{name}' already exists. Skipping creation.")
        return
    print(f"Creating index '{name}'...")
    r.execute_command(
        "FT.CREATE", name,
        "ON", "JSON",
        "PREFIX", "1", KEY_PREFIX,
        "SCHEMA",
        "$.id", "AS", "id", "NUMERIC",
        "$.cnpj", "AS", "cnpj", "TAG",
        "$.company_name", "AS", "company_name", "TEXT", "SORTABLE",
        "$.state", "AS", "state", "TAG",
        "$.channel", "AS", "channel", "TAG",
        "$.network_code", "AS", "network_code", "NUMERIC",
        "$.mcc", "AS", "mcc", "TAG"
    )

# ---- Loader ----
def load_data(r: redis.Redis, total: int, batch: int):
    for start in range(0, total, batch):
        pipe = r.pipeline(transaction=False)
        end = min(start + batch, total)
        for i in range(start + 1, end + 1):
            key = f"{KEY_PREFIX}{i}"
            doc = gen_merchant(i)
            pipe.execute_command("JSON.SET", key, "$", json.dumps(doc, ensure_ascii=False))
        pipe.execute()
        if (end % (batch * 10)) == 0 or end == total:
            print(f"Inserted {end} / {total}")

def main():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    print(f"Connected to Redis: {REDIS_URL}")

    if not CREATE_INDEX_AFTER_LOAD:
        ensure_index(r, INDEX_NAME)

    load_data(r, TOTAL_DOCS, BATCH_SIZE)

    if CREATE_INDEX_AFTER_LOAD:
        ensure_index(r, INDEX_NAME)

    print("Done.")

if __name__ == "__main__":
    main()