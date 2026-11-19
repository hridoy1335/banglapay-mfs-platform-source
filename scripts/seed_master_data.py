
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.generators.master_data import generate_master_data


load_dotenv(PROJECT_ROOT / ".env", override=True)


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing environment variable: {name}")
    return value


def insert_rows(cur, label: str, sql: str, rows: list):
    if not rows:
        print(f"SKIP PostgreSQL {label}: 0 rows")
        return

    cur.executemany(sql, rows)
    print(f"POSTGRESQL {label}: {len(rows)} rows submitted")


def write_postgres(data: dict):
    print("\n[1] Writing master data to Neon PostgreSQL")

    statements = [
        (
            "customers",
            """
            INSERT INTO customer.customers (
                customer_id, customer_ref, customer_type,
                account_status, kyc_status,
                registration_channel, registered_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            data["customers"],
        ),
        (
            "customer_profiles",
            """
            INSERT INTO customer.customer_profiles (
                profile_id, customer_id, display_name,
                mobile_token, email_address,
                district_name, upazila_name, preferred_language
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            data["profiles"],
        ),
        (
            "wallets",
            """
            INSERT INTO wallet.wallets (
                wallet_id, wallet_ref, customer_id,
                wallet_type, wallet_status, currency, opened_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            data["wallets"],
        ),
        (
            "wallet_balances",
            """
            INSERT INTO wallet.wallet_balances (
                wallet_id, book_balance, available_balance,
                held_balance, balance_version
            )
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            data["balances"],
        ),
        (
            "agents",
            """
            INSERT INTO network.agents (
                agent_id, agent_ref, agent_name, agent_type,
                business_type, agent_status, onboarding_date,
                district_name, upazila_name
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            data["agents"],
        ),
        (
            "agent_outlets",
            """
            INSERT INTO network.agent_outlets (
                outlet_id, outlet_ref, agent_id, outlet_name,
                district_name, upazila_name, outlet_status, opened_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            data["outlets"],
        ),
        (
            "agent_float_accounts",
            """
            INSERT INTO network.agent_float_accounts (
                float_account_id, float_account_ref, outlet_id,
                currency, electronic_float_balance,
                reserved_electronic_float, physical_cash_balance
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            data["floats"],
        ),
        (
            "merchants",
            """
            INSERT INTO merchant.merchants (
                merchant_id, merchant_ref, merchant_name,
                merchant_category, merchant_type,
                merchant_status, settlement_frequency,
                district_name, onboarding_date
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            data["merchants"],
        ),
        (
            "merchant_terminals",
            """
            INSERT INTO merchant.merchant_terminals (
                terminal_id, terminal_ref, merchant_id,
                terminal_type, terminal_status
            )
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            data["terminals"],
        ),
        (
            "service_providers",
            """
            INSERT INTO payment.service_providers (
                provider_id, provider_ref,
                provider_name, provider_type, provider_status
            )
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            data["providers"],
        ),
    ]

    # One PostgreSQL transaction for all master records.
    with psycopg.connect(
        required_env("NEON_DATABASE_URL"),
        connect_timeout=20
    ) as conn:

        with conn.cursor() as cur:
            for label, sql, rows in statements:
                insert_rows(cur, label, sql, rows)

        # Connection context commits after successful completion.

    print("PASS: Neon master data transaction committed")


def write_mongodb(data: dict):
    print("\n[2] Writing device profiles to MongoDB")

    client = MongoClient(
        required_env("MONGODB_URI"),
        serverSelectionTimeoutMS=15000
    )

    try:
        client.admin.command("ping")

        db = client[
            os.getenv("MONGODB_DATABASE", "banglapay_ops")
        ]

        collection = db["device_profiles"]

        operations = [
            UpdateOne(
                {"device_id": document["device_id"]},
                {"$setOnInsert": document},
                upsert=True
            )
            for document in data["devices"]
        ]

        inserted = 0

        # Bounded batches to keep M0 workload controlled.
        for start in range(0, len(operations), 100):
            batch = operations[start:start + 100]
            result = collection.bulk_write(
                batch,
                ordered=False
            )
            inserted += result.upserted_count

        print(f"MongoDB new device profiles: {inserted}")
        print(
            "MongoDB total device profiles:",
            collection.count_documents({})
        )

    finally:
        client.close()

    print("PASS: MongoDB master data operation completed")


def show_summary(data: dict):
    print("\n========== MASTER DATA SUMMARY ==========")

    for name, rows in data.items():
        print(f"{name}: {len(rows)}")


def main():
    parser = argparse.ArgumentParser(
        description="BanglaPay MFS master data seeder"
    )

    parser.add_argument("--customers", type=int, default=500)
    parser.add_argument("--agents", type=int, default=20)
    parser.add_argument("--outlets-per-agent", type=int, default=2)
    parser.add_argument("--merchants", type=int, default=50)
    parser.add_argument("--terminals", type=int, default=75)
    parser.add_argument("--devices", type=int, default=400)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate and preview data without database writes"
    )

    args = parser.parse_args()

    if args.merchants == 0 and args.terminals > 0:
        parser.error("Terminals require at least one merchant")

    if args.devices > args.customers:
        parser.error(
            "This initial seed supports at most one device per customer"
        )

    data = generate_master_data(
        customers_count=args.customers,
        agents_count=args.agents,
        outlets_per_agent=args.outlets_per_agent,
        merchants_count=args.merchants,
        terminals_count=args.terminals,
        devices_count=args.devices,
    )

    print("BanglaPay Master Data Generator")
    show_summary(data)

    if args.dry_run:
        print("\nDRY RUN: No database writes performed.")
        return 0

    try:
        write_postgres(data)
        write_mongodb(data)

    except Exception as exc:
        print(
            f"\nMASTER DATA SEED FAILED: "
            f"{type(exc).__name__}: {exc}"
        )
        print(
            "You can rerun safely using the same parameters. "
            "Previously inserted deterministic IDs are retained."
        )
        return 1

    print("\nALL MASTER DATA OPERATIONS COMPLETED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
