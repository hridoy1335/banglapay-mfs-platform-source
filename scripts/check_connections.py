import os
import sys

import psycopg
from dotenv import load_dotenv
from pymongo import MongoClient
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient

load_dotenv()

TOPICS = [
    "mfs.transaction.events.v1",
    "mfs.wallet.events.v1",
    "mfs.agent.events.v1",
    "mfs.merchant.events.v1",
    "mfs.settlement.events.v1",
    "mfs.customer.activity.v1",
    "mfs.risk.events.v1",
]

SUBJECTS = [f"{topic}-value" for topic in TOPICS]


def env(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing environment variable: {name}")
    return value

def check_neon():
    expected_tables = 22

    with psycopg.connect(
        env("NEON_DATABASE_URL"),
        connect_timeout=15
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            database = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema IN (
                    'customer', 'wallet', 'network',
                    'merchant', 'payment', 'accounting',
                    'settlement', 'integration'
                )
                AND table_type = 'BASE TABLE'
            """)
            tables = cur.fetchone()[0]

    passed = tables == expected_tables
    status = "PASS" if passed else "FAIL"

    print(
        f"{status} Neon: {database}, "
        f"{tables} tables (expected {expected_tables})"
    )

    return passed


def check_mongodb():
    client = MongoClient(
        env("MONGODB_URI"),
        serverSelectionTimeoutMS=15000
    )

    try:
        client.admin.command("ping")
        db = client[os.getenv("MONGODB_DATABASE", "banglapay_ops")]
        collections = db.list_collection_names()

        print(f"PASS MongoDB: {db.name}, {len(collections)} collections")
        return len(collections) == 6
    finally:
        client.close()


def check_kafka():
    admin = AdminClient({
        "bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": env("KAFKA_API_KEY"),
        "sasl.password": env("KAFKA_API_SECRET"),
    })

    metadata = admin.list_topics(timeout=15)
    existing = set(metadata.topics)
    missing = set(TOPICS) - existing

    if missing:
        print(f"FAIL Kafka: Missing topics: {sorted(missing)}")
        return False

    print(f"PASS Kafka: {len(TOPICS)} expected topics available")
    return True


def check_schema_registry():
    client = SchemaRegistryClient({
        "url": env("SCHEMA_REGISTRY_URL"),
        "basic.auth.user.info": (
            f"{env('SCHEMA_REGISTRY_API_KEY')}:"
            f"{env('SCHEMA_REGISTRY_API_SECRET')}"
        )
    })

    existing = set(client.get_subjects())
    missing = set(SUBJECTS) - existing

    if missing:
        print(f"FAIL Schema Registry: Missing {sorted(missing)}")
        return False

    print(f"PASS Schema Registry: {len(SUBJECTS)} subjects available")
    return True


def main():
    checks = [
        ("Neon PostgreSQL", check_neon),
        ("MongoDB Atlas", check_mongodb),
        ("Confluent Kafka", check_kafka),
        ("Schema Registry", check_schema_registry),
    ]

    results = {}

    for name, function in checks:
        try:
            results[name] = function()
        except Exception as error:
            results[name] = False
            print(f"FAIL {name}: {type(error).__name__}: {error}")

    print("\n========== VALIDATION SUMMARY ==========")

    for name, passed in results.items():
        print(f"{name}: {'PASS' if passed else 'FAIL'}")

    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())