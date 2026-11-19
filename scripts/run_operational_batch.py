from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from uuid import UUID, uuid4

from dotenv import load_dotenv
from pymongo import MongoClient, ReplaceOne
from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from src.generators.operational_data import (
    build_operational_documents,
)


COLLECTION_NAMES = [
    "app_sessions",
    "customer_activities",
    "fraud_cases",
    "support_cases",
    "merchant_device_events",
]


def require_env(name: str) -> str:
    result = os.getenv(name)
    if not result:
        raise RuntimeError(f"Missing environment variable: {name}")
    return result


def load_postgres_data():
    database_url = require_env("NEON_DATABASE_URL")

    with connect(database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    c.customer_id,
                    w.wallet_id
                FROM customer.customers c
                JOIN wallet.wallets w
                    ON w.customer_id = c.customer_id
                ORDER BY c.customer_id
            """)
            customers = cur.fetchall()

            cur.execute("""
                SELECT
                    t.*,
                    sw.customer_id AS sender_customer_id,
                    rw.customer_id AS receiver_customer_id
                FROM payment.transactions t
                LEFT JOIN wallet.wallets sw
                    ON sw.wallet_id = t.sender_wallet_id
                LEFT JOIN wallet.wallets rw
                    ON rw.wallet_id = t.receiver_wallet_id
                ORDER BY t.initiated_at
            """)
            transactions = cur.fetchall()

            cur.execute("""
                SELECT *
                FROM merchant.merchant_terminals
            """)
            terminals = cur.fetchall()

    return customers, transactions, terminals


def create_indexes(database):
    database.app_sessions.create_index("run_id")
    database.app_sessions.create_index([
        ("customer_id", 1),
        ("started_at", -1),
    ])

    database.customer_activities.create_index("run_id")
    database.customer_activities.create_index([
        ("customer_id", 1),
        ("occurred_at", -1),
    ])
    database.customer_activities.create_index([
        ("session_id", 1),
        ("occurred_at", 1),
    ])

    database.fraud_cases.create_index("run_id")
    database.fraud_cases.create_index([
        ("case_status", 1),
        ("risk_score", -1),
    ])
    database.fraud_cases.create_index("transaction_id")

    database.support_cases.create_index("run_id")
    database.support_cases.create_index([
        ("customer_id", 1),
        ("status", 1),
    ])

    database.merchant_device_events.create_index("run_id")
    database.merchant_device_events.create_index([
        ("terminal_id", 1),
        ("occurred_at", -1),
    ])


def register_run(run_id: UUID, requested_count: int, metadata: dict):
    database_url = require_env("NEON_DATABASE_URL")

    with connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO integration.source_runs (
                    run_id,
                    run_type,
                    run_status,
                    requested_count,
                    metadata
                )
                VALUES (
                    %s,
                    'OPERATIONAL_BATCH',
                    'RUNNING',
                    %s,
                    %s
                )
            """, (
                run_id,
                requested_count,
                Jsonb(metadata),
            ))


def complete_run(run_id: UUID, created_count: int, metadata: dict):
    database_url = require_env("NEON_DATABASE_URL")

    with connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE integration.source_runs
                SET
                    run_status = 'COMPLETED',
                    created_count = %s,
                    failed_count = 0,
                    completed_at = NOW(),
                    metadata = metadata || %s
                WHERE run_id = %s
            """, (
                created_count,
                Jsonb(metadata),
                run_id,
            ))

            if cur.rowcount != 1:
                raise RuntimeError("Could not complete source run.")


def fail_run(
    run_id: UUID,
    written_count: int,
    error: Exception,
):
    database_url = require_env("NEON_DATABASE_URL")

    status = "PARTIAL" if written_count > 0 else "FAILED"

    with connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE integration.source_runs
                SET
                    run_status = %s,
                    created_count = %s,
                    completed_at = NOW(),
                    error_summary = %s
                WHERE run_id = %s
            """, (
                status,
                written_count,
                str(error)[:1000],
                run_id,
            ))


def upsert_documents(database, documents: dict[str, list[dict]]):
    total = 0

    for collection_name, rows in documents.items():
        collection = database[collection_name]

        if not rows:
            print(f"{collection_name}: 0 documents")
            continue

        operations = [
            ReplaceOne(
                {"_id": document["_id"]},
                document,
                upsert=True,
            )
            for document in rows
        ]

        collection_written = 0

        # Small batches are gentler on the MongoDB Free cluster.
        for start in range(0, len(operations), 200):
            chunk = operations[start:start + 200]
            result = collection.bulk_write(chunk, ordered=False)

            collection_written += (
                result.upserted_count
                + result.modified_count
                + result.matched_count
            )

        total += collection_written
        print(
            f"{collection_name}: "
            f"{len(rows)} documents processed"
        )

    return total


def count_run_documents(database, run_id: UUID) -> int:
    run_key = str(run_id)
    return sum(
        database[name].count_documents({"run_id": run_key})
        for name in COLLECTION_NAMES
    )


def main():
    parser = argparse.ArgumentParser(
        description="Generate BanglaPay MongoDB operational data."
    )

    parser.add_argument("--sessions", type=int, default=300)
    parser.add_argument("--activities", type=int, default=1000)
    parser.add_argument("--fraud-cases", type=int, default=10)
    parser.add_argument("--support-cases", type=int, default=20)
    parser.add_argument("--merchant-events", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    counts = [
        args.sessions,
        args.activities,
        args.fraud_cases,
        args.support_cases,
        args.merchant_events,
    ]

    if any(count < 0 for count in counts):
        parser.error("Batch counts cannot be negative.")

    run_id = uuid4()

    print("\n========== BANGLAPAY OPERATIONAL BATCH ==========")
    print(f"Run ID: {run_id}")

    print("\nLoading PostgreSQL source data...")
    customers, transactions, terminals = load_postgres_data()

    print(f"Customers/wallets: {len(customers)}")
    print(f"Transactions: {len(transactions)}")
    print(f"Merchant terminals: {len(terminals)}")

    mongo_uri = require_env("MONGODB_URI")
    database_name = require_env("MONGODB_DATABASE")

    client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=10000,
        appname="BanglaPay-Operational-Generator",
    )

    database = client[database_name]

    try:
        client.admin.command("ping")
        device_profiles = list(
            database.device_profiles.find({})
        )

        print(f"Existing device profiles: {len(device_profiles)}")

        documents = build_operational_documents(
            run_id=run_id,
            customers=customers,
            transactions=transactions,
            terminals=terminals,
            device_profiles=device_profiles,
            session_count=args.sessions,
            activity_count=args.activities,
            fraud_count=args.fraud_cases,
            support_count=args.support_cases,
            merchant_event_count=args.merchant_events,
        )

        document_counts = {
            name: len(rows)
            for name, rows in documents.items()
        }

        requested_count = sum(document_counts.values())

        print("\nDocument allocation:")
        for name, count in document_counts.items():
            print(f"  {name}: {count}")

        print(f"\nTotal: {requested_count}")

        if args.dry_run:
            print("\nDRY RUN: No PostgreSQL or MongoDB data written.")
            return

        register_run(
            run_id,
            requested_count,
            {
                "generator": "banglapay_operational_data_v1",
                "collections": document_counts,
                "simulation": True,
            },
        )

        written_count = 0

        try:
            create_indexes(database)

            written_count = upsert_documents(
                database,
                documents,
            )

            complete_run(
                run_id,
                requested_count,
                {
                    "mongo_documents_processed": written_count,
                    "collections": document_counts,
                },
            )

        except Exception as exc:
            # Count persisted documents in case a bulk write partially succeeded.
            persisted = count_run_documents(database, run_id)

            try:
                fail_run(run_id, persisted, exc)
            except Exception as status_error:
                print(f"Could not update run status: {status_error}")

            raise

        print("\n========== OPERATIONAL BATCH COMMITTED ==========")
        print(f"Run ID: {run_id}")
        print(f"Documents processed: {written_count}")
        print("MongoDB operational data created successfully.")

    finally:
        client.close()


if __name__ == "__main__":
    main()