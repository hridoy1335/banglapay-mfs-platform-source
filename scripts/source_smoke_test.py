
import os
import sys
import uuid
from pathlib import Path
from datetime import datetime, timezone

import psycopg
from dotenv import load_dotenv
from pymongo import MongoClient

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField
)


# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env", override=True)

RUN_ID = f"SMOKE-{uuid.uuid4().hex[:12]}"


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing environment variable: {name}")
    return value


# --------------------------------------------------
# 1. Neon PostgreSQL: Insert / Read / Delete
# --------------------------------------------------

def test_neon():
    print("\n[1] Neon PostgreSQL write/read test")

    customer_ref = f"SMOKE-{uuid.uuid4().hex[:12]}"
    
    try:
        with psycopg.connect(
            required_env("NEON_DATABASE_URL"),
            connect_timeout=15
        ) as conn:

            with conn.cursor() as cur:

                cur.execute(
                    """
                    INSERT INTO customer.customers (
                        customer_ref,
                        customer_type,
                        account_status,
                        kyc_status,
                        registration_channel
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING customer_id
                    """,
                    (
                        customer_ref,
                        "INDIVIDUAL",
                        "PENDING_KYC",
                        "NOT_STARTED",
                        "MOBILE_APP"
                    )
                )

                customer_id = cur.fetchone()[0]

                cur.execute(
                    """
                    SELECT customer_ref, account_status
                    FROM customer.customers
                    WHERE customer_id = %s
                    """,
                    (customer_id,)
                )

                result = cur.fetchone()

                if not result:
                    raise RuntimeError("PostgreSQL read validation failed")

                cur.execute(
                    """
                    DELETE FROM customer.customers
                    WHERE customer_id = %s
                    """,
                    (customer_id,)
                )

                if cur.rowcount != 1:
                    raise RuntimeError("PostgreSQL cleanup failed")

        print("PASS: Neon INSERT / SELECT / DELETE")
        return True

    except Exception as exc:
        print(f"FAIL: Neon - {type(exc).__name__}: {exc}")
        return False


# --------------------------------------------------
# 2. MongoDB: Insert / Read / Delete
# --------------------------------------------------

def test_mongodb():
    print("\n[2] MongoDB write/read test")

    client = None
    device_id = f"SMOKE-{uuid.uuid4().hex[:12]}"

    try:
        client = MongoClient(
            required_env("MONGODB_URI"),
            serverSelectionTimeoutMS=15000
        )

        database = client[
            os.getenv("MONGODB_DATABASE", "banglapay_ops")
        ]

        collection = database["device_profiles"]

        now = datetime.now(timezone.utc)

        document = {
            "device_id": device_id,
            "customer_id": f"SMOKE-CUSTOMER-{uuid.uuid4().hex[:8]}",
            "device_type": "ANDROID",
            "platform": "MOBILE",
            "os_version": "Android 15",
            "app_version": "1.0.0",
            "trust_status": "TRUSTED",
            "registered_at": now,
            "last_seen_at": now
        }

        inserted = False

        try:
            collection.insert_one(document)
            inserted = True

            result = collection.find_one(
                {"device_id": device_id},
                {"_id": 0, "device_id": 1}
            )

            if result is None:
                raise RuntimeError("MongoDB read validation failed")

        finally:
            if inserted:
                collection.delete_one({"device_id": device_id})

        print("PASS: MongoDB INSERT / FIND / DELETE")
        return True

    except Exception as exc:
        print(f"FAIL: MongoDB - {type(exc).__name__}: {exc}")
        return False

    finally:
        if client:
            client.close()


# --------------------------------------------------
# 3. Kafka + Schema Registry: Avro Publish
# --------------------------------------------------

def test_kafka():
    print("\n[3] Confluent Kafka Avro publish test")

    producer = None

    try:
        registry = SchemaRegistryClient({
            "url": required_env("SCHEMA_REGISTRY_URL"),
            "basic.auth.user.info": (
                f"{required_env('SCHEMA_REGISTRY_API_KEY')}:"
                f"{required_env('SCHEMA_REGISTRY_API_SECRET')}"
            )
        })

        topic = "mfs.transaction.events.v1"
        subject = f"{topic}-value"

        latest = registry.get_latest_version(subject)
        serializer = AvroSerializer(
            registry,
            latest.schema.schema_str
        )

        now = datetime.now(timezone.utc)
        event_id = str(uuid.uuid4())
        transaction_id = str(uuid.uuid4())

        event = {
            "event_id": event_id,
            "event_type": "CONNECTIVITY_TEST",
            "event_version": 1,
            "occurred_at": now,
            "source_system": "BANGAPAY_SMOKE_TEST",
            "aggregate_type": "TRANSACTION",
            "aggregate_id": transaction_id,
            "correlation_id": RUN_ID,
            "run_id": RUN_ID,

            "payload": {
                "transaction_id": transaction_id,
                "transaction_ref": f"SMOKE-{uuid.uuid4().hex[:12]}",
                "transaction_type": "P2P",
                "transaction_status": "SUCCESS",
                "amount_paisa": 100,
                "currency": "BDT",
                "sender_wallet_id": "SMOKE-SENDER",
                "receiver_wallet_id": "SMOKE-RECEIVER",
                "agent_outlet_id": None,
                "merchant_id": None,
                "terminal_id": None,
                "service_provider_id": None,
                "channel": "MOBILE_APP",
                "initiated_at": now,
                "completed_at": now,
                "related_transaction_id": None
            }
        }

        serialized = serializer(
            event,
            SerializationContext(topic, MessageField.VALUE)
        )

        delivery = {}

        def callback(error, message):
            delivery["error"] = error

            if error is None:
                delivery["partition"] = message.partition()
                delivery["offset"] = message.offset()

        producer = Producer({
            "bootstrap.servers": required_env(
                "KAFKA_BOOTSTRAP_SERVERS"
            ),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": required_env("KAFKA_API_KEY"),
            "sasl.password": required_env("KAFKA_API_SECRET"),
            "client.id": "banglapay-smoke-test",
            "acks": "all",
            "enable.idempotence": True
        })

        producer.produce(
            topic,
            key=transaction_id.encode("utf-8"),
            value=serialized,
            on_delivery=callback
        )

        remaining = producer.flush(30)

        if remaining:
            raise TimeoutError(
                f"{remaining} Kafka message(s) not delivered"
            )

        if delivery.get("error"):
            raise RuntimeError(str(delivery["error"]))

        if "partition" not in delivery:
            raise RuntimeError("Kafka delivery confirmation missing")

        print(f"PASS: Avro event published to {topic}")
        print(f"Event ID: {event_id}")
        print(f"Partition: {delivery['partition']}")
        print(f"Offset: {delivery['offset']}")

        return True

    except Exception as exc:
        print(f"FAIL: Kafka - {type(exc).__name__}: {exc}")
        return False

    finally:
        if producer:
            producer.flush(10)


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():
    print("==========================================")
    print("BanglaPay Source Smoke Test")
    print(f"Run ID: {RUN_ID}")
    print("==========================================")

    results = {
        "Neon PostgreSQL": test_neon(),
        "MongoDB Atlas": test_mongodb(),
        "Confluent Kafka + Avro": test_kafka()
    }

    print("\n========== FINAL RESULT ==========")

    for name, passed in results.items():
        print(f"{name}: {'PASS' if passed else 'FAIL'}")

    if all(results.values()):
        print("\nAll source write tests passed!")
        return 0

    print("\nSome source write tests failed.")
    return 1


if __name__ == "__main__":
    sys.exit(main())