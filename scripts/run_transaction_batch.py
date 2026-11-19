
from __future__ import annotations

import argparse
import os
import random
import sys
import uuid

from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
import yaml

from dotenv import load_dotenv
from psycopg.types.json import Jsonb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env", override=True)


def env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing environment variable: {name}")
    return value


DATABASE_URL = env("NEON_DATABASE_URL")

with open(ROOT / "configs/transaction_rules.yaml") as file:
    RULES = yaml.safe_load(file)


TX_WEIGHTS = RULES["transaction_distribution"]
STATUS_WEIGHTS = RULES["status_distribution"]
AMOUNT_RANGES = RULES["amount_ranges_taka"]

TOPICS = {
    "TRANSACTION": "mfs.transaction.events.v1",
    "WALLET": "mfs.wallet.events.v1",
    "AGENT": "mfs.agent.events.v1",
    "MERCHANT": "mfs.merchant.events.v1",
}

JOURNAL_TYPES = {
    "P2P": "P2P_TRANSFER",
    "CASH_IN": "CASH_IN",
    "CASH_OUT": "CASH_OUT",
    "MERCHANT_PAYMENT": "MERCHANT_PAYMENT",
    "BILL_PAYMENT": "BILL_PAYMENT",
    "MOBILE_RECHARGE": "BILL_PAYMENT",
    "SALARY_DISBURSEMENT": "SALARY_DISBURSEMENT",
}


def to_paisa(value: Decimal) -> int:
    return int(value * Decimal("100"))


def from_paisa(value: int) -> Decimal:
    return Decimal(value) / Decimal("100")


def allocate_counts(total: int, weights: dict) -> dict:
    """Allocate an exact total using largest-remainder allocation."""

    weight_total = sum(weights.values())

    raw = {
        key: total * weight / weight_total
        for key, weight in weights.items()
    }

    counts = {
        key: int(value)
        for key, value in raw.items()
    }

    remaining = total - sum(counts.values())

    order = sorted(
        weights,
        key=lambda key: raw[key] - counts[key],
        reverse=True
    )

    for key in order[:remaining]:
        counts[key] += 1

    return counts


def shuffled_values(total: int, weights: dict, rng) -> list:
    counts = allocate_counts(total, weights)

    values = [
        value
        for value, count in counts.items()
        for _ in range(count)
    ]

    rng.shuffle(values)
    return values


def validate_schema(cur):
    cur.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'integration'
              AND table_name = 'outbox_events'
              AND column_name = 'topic_name'
        )
    """)

    if not cur.fetchone()[0]:
        raise RuntimeError(
            "Outbox topic_name migration is missing."
        )


def validate_financial_opening(cur):
    """Validate the initial journal and operational balances."""

    for ref in [
        "OPENING-INITIAL-001",
        "OPENING-CORPORATE-FUNDING-001"
    ]:
        cur.execute("""
            SELECT COUNT(*)
            FROM accounting.journal_entries
            WHERE journal_ref = %s
              AND journal_status = 'POSTED'
        """, (ref,))

        if cur.fetchone()[0] != 1:
            raise RuntimeError(
                f"Required posted journal missing: {ref}"
            )

    # All previously posted journals must balance.
    cur.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT
                le.journal_id
            FROM accounting.ledger_entries le
            JOIN accounting.journal_entries je
                ON je.journal_id = le.journal_id
            WHERE je.journal_status = 'POSTED'
            GROUP BY le.journal_id
            HAVING
                COALESCE(
                    SUM(le.amount) FILTER (
                        WHERE le.entry_side = 'DEBIT'
                    ), 0
                )
                <>
                COALESCE(
                    SUM(le.amount) FILTER (
                        WHERE le.entry_side = 'CREDIT'
                    ), 0
                )
        ) unbalanced
    """)

    if cur.fetchone()[0] != 0:
        raise RuntimeError("An existing journal is unbalanced.")

    # Wallet operational balances must equal posted
    # customer-wallet liability balances.
    cur.execute("""
        WITH ledger AS (
            SELECT
                le.account_id,
                SUM(
                    CASE
                        WHEN le.entry_side = 'CREDIT'
                            THEN le.amount
                        ELSE -le.amount
                    END
                ) AS balance
            FROM accounting.ledger_entries le
            JOIN accounting.journal_entries je
                ON je.journal_id = le.journal_id
            WHERE je.journal_status = 'POSTED'
            GROUP BY le.account_id
        )
        SELECT COUNT(*)
        FROM wallet.wallet_balances b
        JOIN accounting.ledger_accounts a
            ON a.wallet_id = b.wallet_id
        LEFT JOIN ledger l
            ON l.account_id = a.account_id
        WHERE b.book_balance <> COALESCE(l.balance, 0)
    """)

    if cur.fetchone()[0] != 0:
        raise RuntimeError("Wallet/ledger reconciliation failed.")

    # Agent electronic float must reconcile with its liability account.
    cur.execute("""
        WITH ledger AS (
            SELECT
                le.account_id,
                SUM(
                    CASE
                        WHEN le.entry_side = 'CREDIT'
                            THEN le.amount
                        ELSE -le.amount
                    END
                ) AS balance
            FROM accounting.ledger_entries le
            JOIN accounting.journal_entries je
                ON je.journal_id = le.journal_id
            WHERE je.journal_status = 'POSTED'
            GROUP BY le.account_id
        )
        SELECT COUNT(*)
        FROM network.agent_float_accounts f
        JOIN accounting.ledger_accounts a
            ON a.agent_float_account_id = f.float_account_id
        LEFT JOIN ledger l
            ON l.account_id = a.account_id
        WHERE f.electronic_float_balance <> COALESCE(l.balance, 0)
    """)

    if cur.fetchone()[0] != 0:
        raise RuntimeError("Agent float/ledger reconciliation failed.")


def load_context(cur) -> dict:
    """Load active business participants and lock mutable balances."""

    cur.execute("""
        SELECT
            w.wallet_id,
            w.customer_id,
            la.account_id,
            b.book_balance,
            b.available_balance,
            b.held_balance,
            b.balance_version
        FROM wallet.wallets w
        JOIN wallet.wallet_balances b
            ON b.wallet_id = w.wallet_id
        JOIN accounting.ledger_accounts la
            ON la.wallet_id = w.wallet_id
           AND la.owner_type = 'WALLET'
        WHERE w.wallet_status = 'ACTIVE'
          AND la.is_active = TRUE
        ORDER BY w.wallet_ref
        FOR UPDATE OF b
    """)

    wallets = []

    for row in cur.fetchall():
        wallets.append({
            "wallet_id": row[0],
            "customer_id": row[1],
            "account_id": row[2],
            "book_paisa": to_paisa(row[3]),
            "available_paisa": to_paisa(row[4]),
            "held_paisa": to_paisa(row[5]),
            "balance_version": row[6],
        })

    cur.execute("""
        SELECT
            a.agent_id,
            o.outlet_id,
            f.float_account_id,
            la.account_id,
            f.electronic_float_balance,
            f.reserved_electronic_float,
            f.physical_cash_balance,
            o.outlet_status
        FROM network.agent_float_accounts f
        JOIN network.agent_outlets o
            ON o.outlet_id = f.outlet_id
        JOIN network.agents a
            ON a.agent_id = o.agent_id
        JOIN accounting.ledger_accounts la
            ON la.agent_float_account_id = f.float_account_id
           AND la.owner_type = 'AGENT_FLOAT'
        WHERE a.agent_status = 'ACTIVE'
          AND o.outlet_status = 'ACTIVE'
          AND la.is_active = TRUE
        ORDER BY f.float_account_ref
        FOR UPDATE OF f
    """)

    agents = []

    for row in cur.fetchall():
        agents.append({
            "agent_id": row[0],
            "outlet_id": row[1],
            "float_account_id": row[2],
            "account_id": row[3],
            "electronic_paisa": to_paisa(row[4]),
            "reserved_paisa": to_paisa(row[5]),
            "physical_paisa": to_paisa(row[6]),
            "outlet_status": row[7],
        })

    cur.execute("""
        SELECT
            m.merchant_id,
            la.account_id,
            t.terminal_id,
            t.terminal_type
        FROM merchant.merchants m
        JOIN accounting.ledger_accounts la
            ON la.merchant_id = m.merchant_id
           AND la.owner_type = 'MERCHANT'
        LEFT JOIN merchant.merchant_terminals t
            ON t.merchant_id = m.merchant_id
           AND t.terminal_status = 'ACTIVE'
        WHERE m.merchant_status = 'ACTIVE'
          AND la.is_active = TRUE
        ORDER BY m.merchant_ref
    """)

    merchant_map = {}

    for row in cur.fetchall():
        merchant = merchant_map.setdefault(
            row[0],
            {
                "merchant_id": row[0],
                "account_id": row[1],
                "terminals": [],
            }
        )

        if row[2] is not None:
            merchant["terminals"].append({
                "terminal_id": row[2],
                "terminal_type": row[3],
            })

    merchants = list(merchant_map.values())

    # Calculate provider liability balances from the posted ledger.
    cur.execute("""
        WITH balances AS (
            SELECT
                le.account_id,
                SUM(
                    CASE
                        WHEN le.entry_side = 'CREDIT'
                            THEN le.amount
                        ELSE -le.amount
                    END
                ) AS balance
            FROM accounting.ledger_entries le
            JOIN accounting.journal_entries je
                ON je.journal_id = le.journal_id
            WHERE je.journal_status = 'POSTED'
            GROUP BY le.account_id
        )
        SELECT
            p.provider_id,
            p.provider_ref,
            p.provider_type,
            la.account_id,
            COALESCE(b.balance, 0)
        FROM payment.service_providers p
        JOIN accounting.ledger_accounts la
            ON la.provider_id = p.provider_id
           AND la.owner_type = 'PROVIDER'
        LEFT JOIN balances b
            ON b.account_id = la.account_id
        WHERE p.provider_status = 'ACTIVE'
          AND la.is_active = TRUE
        ORDER BY p.provider_ref
    """)

    providers = []

    for row in cur.fetchall():
        providers.append({
            "provider_id": row[0],
            "provider_ref": row[1],
            "provider_type": row[2],
            "account_id": row[3],
            "balance_paisa": to_paisa(row[4]),
        })

    if not wallets or not agents or not merchants or not providers:
        raise RuntimeError("One or more active source pools are empty.")

    corporate = [
        p for p in providers
        if p["provider_type"] == "CORPORATE"
    ]

    if not any(p["balance_paisa"] > 0 for p in corporate):
        raise RuntimeError("No funded corporate provider available.")

    return {
        "wallets": wallets,
        "agents": agents,
        "merchants": merchants,
        "providers": providers,
        "corporate": corporate,
        "billers": [
            p for p in providers
            if p["provider_type"] in ("BILLER", "GOVERNMENT")
        ],
        "operators": [
            p for p in providers
            if p["provider_type"] == "MOBILE_OPERATOR"
        ],
    }


def amount_limits(tx_type: str) -> tuple[int, int]:
    low, high = AMOUNT_RANGES[tx_type]
    return int(low * 100), int(high * 100)


def random_amount(rng, tx_type: str) -> int:
    low, high = amount_limits(tx_type)
    return rng.randint(low, high)


def choose_debit_wallet(ctx, rng, tx_type):
    low, high = amount_limits(tx_type)

    candidates = [
        w for w in ctx["wallets"]
        if w["available_paisa"] >= low
    ]

    if not candidates:
        return None

    wallet = rng.choice(candidates)
    cap = min(high, wallet["available_paisa"])

    return wallet, rng.randint(low, cap)


def choose_provider(ctx, rng, provider_type):
    options = [
        p for p in ctx["providers"]
        if p["provider_type"] == provider_type
    ]
    return rng.choice(options) if options else None


def build_success(ctx, rng, tx_type):
    wallets = ctx["wallets"]
    agents = ctx["agents"]
    low, high = amount_limits(tx_type)

    base = {
        "sender": None,
        "receiver": None,
        "agent": None,
        "merchant": None,
        "terminal": None,
        "provider": None,
        "debit_account": None,
        "credit_account": None,
        "wallet_changes": [],
        "agent_change": None,
        "funding_provider": None,
    }

    if tx_type == "P2P":
        selected = choose_debit_wallet(ctx, rng, tx_type)
        if not selected:
            return None

        sender, amount = selected
        receiver_options = [
            w for w in wallets
            if w["wallet_id"] != sender["wallet_id"]
        ]

        if not receiver_options:
            return None

        receiver = rng.choice(receiver_options)

        base.update({
            "sender": sender,
            "receiver": receiver,
            "debit_account": sender["account_id"],
            "credit_account": receiver["account_id"],
            "wallet_changes": [
                ("DEBIT", sender),
                ("CREDIT", receiver),
            ],
            "amount_paisa": amount,
        })

    elif tx_type == "CASH_IN":
        candidates = [
            a for a in agents
            if a["electronic_paisa"] >= low
        ]

        if not candidates:
            return None

        agent = rng.choice(candidates)
        receiver = rng.choice(wallets)

        amount = rng.randint(
            low,
            min(high, agent["electronic_paisa"])
        )

        base.update({
            "receiver": receiver,
            "agent": agent,
            "debit_account": agent["account_id"],
            "credit_account": receiver["account_id"],
            "wallet_changes": [("CREDIT", receiver)],
            "agent_change": "CASH_IN",
            "amount_paisa": amount,
        })

    elif tx_type == "CASH_OUT":
        wallet_candidates = [
            w for w in wallets
            if w["available_paisa"] >= low
        ]

        agent_candidates = [
            a for a in agents
            if a["physical_paisa"] >= low
        ]

        if not wallet_candidates or not agent_candidates:
            return None

        for _ in range(30):
            sender = rng.choice(wallet_candidates)
            agent = rng.choice(agent_candidates)

            cap = min(
                high,
                sender["available_paisa"],
                agent["physical_paisa"]
            )

            if cap >= low:
                break
        else:
            return None

        amount = rng.randint(low, cap)

        base.update({
            "sender": sender,
            "agent": agent,
            "debit_account": sender["account_id"],
            "credit_account": agent["account_id"],
            "wallet_changes": [("DEBIT", sender)],
            "agent_change": "CASH_OUT",
            "amount_paisa": amount,
        })

    elif tx_type == "MERCHANT_PAYMENT":
        selected = choose_debit_wallet(ctx, rng, tx_type)
        if not selected:
            return None

        sender, amount = selected
        merchant = rng.choice(ctx["merchants"])
        terminal = (
            rng.choice(merchant["terminals"])
            if merchant["terminals"]
            else None
        )

        base.update({
            "sender": sender,
            "merchant": merchant,
            "terminal": terminal,
            "debit_account": sender["account_id"],
            "credit_account": merchant["account_id"],
            "wallet_changes": [("DEBIT", sender)],
            "amount_paisa": amount,
        })

    elif tx_type in ("BILL_PAYMENT", "MOBILE_RECHARGE"):
        selected = choose_debit_wallet(ctx, rng, tx_type)
        if not selected:
            return None

        sender, amount = selected
        provider = (
            choose_provider(ctx, rng, "BILLER")
            or choose_provider(ctx, rng, "GOVERNMENT")
            if tx_type == "BILL_PAYMENT"
            else choose_provider(ctx, rng, "MOBILE_OPERATOR")
        )

        if not provider:
            return None

        base.update({
            "sender": sender,
            "provider": provider,
            "debit_account": sender["account_id"],
            "credit_account": provider["account_id"],
            "wallet_changes": [("DEBIT", sender)],
            "amount_paisa": amount,
        })

    elif tx_type == "SALARY_DISBURSEMENT":
        candidates = [
            p for p in ctx["corporate"]
            if p["balance_paisa"] >= low
        ]

        if not candidates:
            return None

        provider = rng.choice(candidates)
        receiver = rng.choice(wallets)

        cap = min(high, provider["balance_paisa"])
        amount = rng.randint(low, cap)

        base.update({
            "receiver": receiver,
            "provider": provider,
            "funding_provider": provider,
            "debit_account": provider["account_id"],
            "credit_account": receiver["account_id"],
            "wallet_changes": [("CREDIT", receiver)],
            "amount_paisa": amount,
        })

    else:
        raise ValueError(f"Unsupported type: {tx_type}")

    return base


def build_non_success(ctx, rng, tx_type):
    """Build valid transaction participants without moving funds."""

    wallets = ctx["wallets"]
    agents = ctx["agents"]
    merchants = ctx["merchants"]

    base = {
        "sender": None,
        "receiver": None,
        "agent": None,
        "merchant": None,
        "terminal": None,
        "provider": None,
        "debit_account": None,
        "credit_account": None,
        "wallet_changes": [],
        "agent_change": None,
        "funding_provider": None,
        "amount_paisa": random_amount(rng, tx_type),
    }

    if tx_type == "P2P":
        sender, receiver = rng.sample(wallets, 2)
        base.update(sender=sender, receiver=receiver)

    elif tx_type == "CASH_IN":
        base.update(
            receiver=rng.choice(wallets),
            agent=rng.choice(agents)
        )

    elif tx_type == "CASH_OUT":
        base.update(
            sender=rng.choice(wallets),
            agent=rng.choice(agents)
        )

    elif tx_type == "MERCHANT_PAYMENT":
        merchant = rng.choice(merchants)
        terminal = (
            rng.choice(merchant["terminals"])
            if merchant["terminals"] else None
        )
        base.update(
            sender=rng.choice(wallets),
            merchant=merchant,
            terminal=terminal
        )

    elif tx_type == "BILL_PAYMENT":
        provider = (
            choose_provider(ctx, rng, "BILLER")
            or choose_provider(ctx, rng, "GOVERNMENT")
        )
        base.update(
            sender=rng.choice(wallets),
            provider=provider
        )

    elif tx_type == "MOBILE_RECHARGE":
        base.update(
            sender=rng.choice(wallets),
            provider=choose_provider(ctx, rng, "MOBILE_OPERATOR")
        )

    elif tx_type == "SALARY_DISBURSEMENT":
        base.update(
            receiver=rng.choice(wallets),
            provider=rng.choice(ctx["corporate"])
        )

    return base


def channel_for(tx_type: str, data: dict) -> str:
    if tx_type == "P2P":
        return "MOBILE_APP"

    if tx_type in ("CASH_IN", "CASH_OUT"):
        return "AGENT_PORTAL"

    if tx_type == "SALARY_DISBURSEMENT":
        return "BATCH"

    if tx_type == "MERCHANT_PAYMENT":
        terminal = data["terminal"]

        if not terminal:
            return "MERCHANT_QR"

        return {
            "QR": "MERCHANT_QR",
            "POS": "POS",
            "WEB": "WEB",
            "API": "API",
        }[terminal["terminal_type"]]

    return "MOBILE_APP"


def insert_status_history(cur, transaction_id, status, times, failure_code):
    initial, transition, completed = times

    rows = [
        (
            transaction_id,
            None,
            "INITIATED",
            "REQUEST_ACCEPTED",
            initial,
        )
    ]

    if status == "SUCCESS":
        rows.extend([
            (
                transaction_id,
                "INITIATED",
                "AUTHORIZED",
                "AUTHORIZATION_APPROVED",
                transition,
            ),
            (
                transaction_id,
                "AUTHORIZED",
                "SUCCESS",
                "POSTING_COMPLETED",
                completed,
            ),
        ])

    elif status == "FAILED":
        rows.append((
            transaction_id,
            "INITIATED",
            "FAILED",
            failure_code,
            transition,
        ))

    else:
        rows.append((
            transaction_id,
            "INITIATED",
            "PENDING",
            "PROCESSING_DELAY",
            transition,
        ))

    cur.executemany("""
        INSERT INTO payment.transaction_status_history (
            transaction_id,
            previous_status,
            new_status,
            reason_code,
            changed_by,
            changed_at
        )
        VALUES (%s,%s,%s,%s,'SYSTEM',%s)
    """, rows)


def insert_outbox(
    cur,
    run_id,
    topic,
    event_type,
    aggregate_type,
    aggregate_id,
    partition_key,
    payload,
    occurred_at,
    correlation_id,
):
    event_id = uuid.uuid4()

    envelope = {
        "event_id": str(event_id),
        "event_type": event_type,
        "event_version": 1,
        "occurred_at": occurred_at.isoformat(),
        "source_system": "BANGLAPAY_TRANSACTION_ENGINE",
        "aggregate_type": aggregate_type,
        "aggregate_id": str(aggregate_id),
        "correlation_id": str(correlation_id) if correlation_id else None,
        "run_id": str(run_id),
        "payload": payload,
    }

    cur.execute("""
        INSERT INTO integration.outbox_events (
            event_id,
            dedupe_key,
            aggregate_type,
            aggregate_id,
            event_type,
            event_version,
            topic_name,
            partition_key,
            event_payload,
            publish_status,
            available_at,
            run_id
        )
        VALUES (
            %s,%s,%s,%s,%s,1,%s,%s,%s,'PENDING',%s,%s
        )
    """, (
        event_id,
        f"{run_id}:{event_id}",
        aggregate_type,
        aggregate_id,
        event_type,
        topic,
        partition_key,
        Jsonb(envelope),
        occurred_at,
        run_id,
    ))


def apply_wallet_change(
    cur, run_id, wallet, direction,
    amount_paisa, transaction_id, occurred_at
):
    amount = from_paisa(amount_paisa)

    previous_book = wallet["book_paisa"]
    previous_available = wallet["available_paisa"]

    if direction == "DEBIT":
        if previous_available < amount_paisa:
            raise RuntimeError("Wallet balance changed below required amount.")

        cur.execute("""
            UPDATE wallet.wallet_balances
            SET
                book_balance = book_balance - %s,
                available_balance = available_balance - %s,
                balance_version = balance_version + 1,
                updated_at = NOW()
            WHERE wallet_id = %s
              AND available_balance >= %s
        """, (amount, amount, wallet["wallet_id"], amount))

        if cur.rowcount != 1:
            raise RuntimeError("Wallet debit validation failed.")

        wallet["book_paisa"] -= amount_paisa
        wallet["available_paisa"] -= amount_paisa

    elif direction == "CREDIT":
        cur.execute("""
            UPDATE wallet.wallet_balances
            SET
                book_balance = book_balance + %s,
                available_balance = available_balance + %s,
                balance_version = balance_version + 1,
                updated_at = NOW()
            WHERE wallet_id = %s
        """, (amount, amount, wallet["wallet_id"]))

        if cur.rowcount != 1:
            raise RuntimeError("Wallet credit failed.")

        wallet["book_paisa"] += amount_paisa
        wallet["available_paisa"] += amount_paisa

    else:
        raise ValueError(f"Invalid wallet direction: {direction}")

    wallet["balance_version"] += 1

    payload = {
        "wallet_id": str(wallet["wallet_id"]),
        "customer_id": str(wallet["customer_id"]),
        "change_type": direction,
        "source_transaction_id": str(transaction_id),
        "previous_book_balance_paisa": previous_book,
        "book_balance_paisa": wallet["book_paisa"],
        "previous_available_balance_paisa": previous_available,
        "available_balance_paisa": wallet["available_paisa"],
        "held_balance_paisa": wallet["held_paisa"],
        "balance_version": wallet["balance_version"],
        "currency": "BDT",
    }

    insert_outbox(
        cur, run_id,
        TOPICS["WALLET"],
        "WALLET_BALANCE_CHANGED",
        "WALLET",
        wallet["wallet_id"],
        str(wallet["wallet_id"]),
        payload,
        occurred_at,
        transaction_id,
    )


def apply_agent_change(
    cur, run_id, agent, change_type,
    amount_paisa, transaction_id, occurred_at
):
    amount = from_paisa(amount_paisa)

    if change_type == "CASH_IN":
        if agent["electronic_paisa"] < amount_paisa:
            raise RuntimeError("Agent electronic float insufficient.")

        cur.execute("""
            UPDATE network.agent_float_accounts
            SET
                electronic_float_balance =
                    electronic_float_balance - %s,
                physical_cash_balance =
                    physical_cash_balance + %s,
                updated_at = NOW()
            WHERE float_account_id = %s
              AND electronic_float_balance >= %s
        """, (
            amount, amount,
            agent["float_account_id"],
            amount
        ))

        agent["electronic_paisa"] -= amount_paisa
        agent["physical_paisa"] += amount_paisa

    elif change_type == "CASH_OUT":
        if agent["physical_paisa"] < amount_paisa:
            raise RuntimeError("Agent physical cash insufficient.")

        cur.execute("""
            UPDATE network.agent_float_accounts
            SET
                electronic_float_balance =
                    electronic_float_balance + %s,
                physical_cash_balance =
                    physical_cash_balance - %s,
                updated_at = NOW()
            WHERE float_account_id = %s
              AND physical_cash_balance >= %s
        """, (
            amount, amount,
            agent["float_account_id"],
            amount
        ))

        agent["electronic_paisa"] += amount_paisa
        agent["physical_paisa"] -= amount_paisa

    if cur.rowcount != 1:
        raise RuntimeError("Agent float update failed.")

    payload = {
        "agent_id": str(agent["agent_id"]),
        "outlet_id": str(agent["outlet_id"]),
        "event_category": change_type,
        "electronic_float_paisa": agent["electronic_paisa"],
        "reserved_float_paisa": agent["reserved_paisa"],
        "physical_cash_paisa": agent["physical_paisa"],
        "currency": "BDT",
        "outlet_status": agent["outlet_status"],
    }

    insert_outbox(
        cur, run_id,
        TOPICS["AGENT"],
        "AGENT_FLOAT_CHANGED",
        "AGENT",
        agent["agent_id"],
        str(agent["agent_id"]),
        payload,
        occurred_at,
        transaction_id,
    )


def post_journal(cur, tx_id, tx_ref, tx_type,
                 debit_account, credit_account,
                 amount_paisa, effective_at):
    journal_ref = f"JRN-{tx_ref}"
    amount = from_paisa(amount_paisa)

    cur.execute("""
        INSERT INTO accounting.journal_entries (
            journal_ref,
            source_transaction_id,
            journal_type,
            journal_status,
            currency,
            effective_at,
            posted_at
        )
        VALUES (%s,%s,%s,'POSTED','BDT',%s,%s)
        RETURNING journal_id
    """, (
        journal_ref,
        tx_id,
        JOURNAL_TYPES[tx_type],
        effective_at,
        effective_at,
    ))

    journal_id = cur.fetchone()[0]

    cur.executemany("""
        INSERT INTO accounting.ledger_entries (
            journal_id,
            account_id,
            currency,
            line_number,
            entry_side,
            amount,
            description
        )
        VALUES (%s,%s,'BDT',%s,%s,%s,%s)
    """, [
        (
            journal_id,
            debit_account,
            1,
            "DEBIT",
            amount,
            f"{tx_type} debit: {tx_ref}",
        ),
        (
            journal_id,
            credit_account,
            2,
            "CREDIT",
            amount,
            f"{tx_type} credit: {tx_ref}",
        ),
    ])


def register_run(run_id, total, seed):
    with psycopg.connect(DATABASE_URL, connect_timeout=20) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT run_status
                FROM integration.source_runs
                WHERE run_id = %s
            """, (run_id,))

            existing = cur.fetchone()

            metadata = Jsonb({"seed": seed})

            if existing is None:
                cur.execute("""
                    INSERT INTO integration.source_runs (
                        run_id,
                        run_type,
                        run_status,
                        requested_count,
                        metadata
                    )
                    VALUES (%s,'TRANSACTION_BATCH','RUNNING',%s,%s)
                """, (run_id, total, metadata))

            elif existing[0] == "FAILED":
                cur.execute("""
                    UPDATE integration.source_runs
                    SET
                        run_status = 'RUNNING',
                        requested_count = %s,
                        created_count = 0,
                        failed_count = 0,
                        started_at = NOW(),
                        completed_at = NULL,
                        error_summary = NULL,
                        metadata = %s
                    WHERE run_id = %s
                """, (total, metadata, run_id))

            else:
                raise RuntimeError(
                    f"Run ID already exists with status: {existing[0]}"
                )


def mark_run_failed(run_id, error):
    with psycopg.connect(DATABASE_URL, connect_timeout=20) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE integration.source_runs
                SET
                    run_status = 'FAILED',
                    completed_at = NOW(),
                    error_summary = %s
                WHERE run_id = %s
                  AND run_status = 'RUNNING'
            """, (str(error)[:4000], run_id))


def execute_batch(run_id, total, seed):
    rng = random.Random(seed)

    types = shuffled_values(total, TX_WEIGHTS, rng)
    statuses = shuffled_values(total, STATUS_WEIGHTS, rng)

    actual_types = Counter()
    actual_statuses = Counter()
    outbox_total = 0

    with psycopg.connect(
        DATABASE_URL,
        connect_timeout=20
    ) as conn:

        with conn.cursor() as cur:

            validate_schema(cur)
            validate_financial_opening(cur)

            ctx = load_context(cur)

            print(
                f"Loaded pools: {len(ctx['wallets'])} wallets, "
                f"{len(ctx['agents'])} agent outlets, "
                f"{len(ctx['merchants'])} merchants."
            )

            for index, (tx_type, requested_status) in enumerate(
                zip(types, statuses),
                start=1
            ):
                if requested_status == "SUCCESS":
                    data = build_success(ctx, rng, tx_type)

                    if data is None:
                        status = "FAILED"
                        failure_code = "INSUFFICIENT_FUNDS_OR_LIQUIDITY"
                        data = build_non_success(ctx, rng, tx_type)
                    else:
                        status = "SUCCESS"
                        failure_code = None
                else:
                    status = requested_status
                    failure_code = (
                        rng.choice([
                            "SIMULATED_DECLINE",
                            "NETWORK_TIMEOUT",
                            "LIMIT_EXCEEDED",
                        ])
                        if status == "FAILED"
                        else None
                    )
                    data = build_non_success(ctx, rng, tx_type)

                tx_id = uuid.uuid4()

                run_short = run_id.hex[:8].upper()
                tx_ref = f"TXN-{run_short}-{index:06d}"
                idempotency_key = f"{run_id}:{index:06d}"

                now = datetime.now(timezone.utc)

                initiated_at = now - timedelta(
                    seconds=rng.randint(60, 86400)
                )

                authorized_at = (
                    initiated_at + timedelta(seconds=1)
                    if status == "SUCCESS"
                    else None
                )

                completed_at = (
                    initiated_at + timedelta(
                        seconds=rng.randint(2, 20)
                    )
                    if status == "SUCCESS"
                    else None
                )

                event_time = (
                    completed_at
                    if completed_at
                    else initiated_at + timedelta(seconds=2)
                )

                channel = channel_for(tx_type, data)
                amount_paisa = data["amount_paisa"]

                sender = data["sender"]
                receiver = data["receiver"]
                agent = data["agent"]
                merchant = data["merchant"]
                terminal = data["terminal"]
                provider = data["provider"]

                cur.execute("""
                    INSERT INTO payment.transactions (
                        transaction_id,
                        transaction_ref,
                        idempotency_key,
                        transaction_type,
                        sender_wallet_id,
                        receiver_wallet_id,
                        agent_outlet_id,
                        merchant_id,
                        terminal_id,
                        service_provider_id,
                        amount,
                        currency,
                        transaction_status,
                        channel,
                        initiated_at,
                        authorized_at,
                        completed_at,
                        failure_code,
                        failure_reason,
                        source_run_id
                    )
                    VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,'BDT',%s,%s,%s,%s,%s,%s,%s,%s
                    )
                """, (
                    tx_id,
                    tx_ref,
                    idempotency_key,
                    tx_type,
                    sender["wallet_id"] if sender else None,
                    receiver["wallet_id"] if receiver else None,
                    agent["outlet_id"] if agent else None,
                    merchant["merchant_id"] if merchant else None,
                    terminal["terminal_id"] if terminal else None,
                    provider["provider_id"] if provider else None,
                    from_paisa(amount_paisa),
                    status,
                    channel,
                    initiated_at,
                    authorized_at,
                    completed_at,
                    failure_code,
                    (
                        "Synthetic transaction failure"
                        if status == "FAILED" else None
                    ),
                    run_id,
                ))

                transition_at = initiated_at + timedelta(seconds=1)

                insert_status_history(
                    cur,
                    tx_id,
                    status,
                    (
                        initiated_at,
                        transition_at,
                        completed_at
                    ),
                    failure_code
                )

                if status == "SUCCESS":

                    post_journal(
                        cur,
                        tx_id,
                        tx_ref,
                        tx_type,
                        data["debit_account"],
                        data["credit_account"],
                        amount_paisa,
                        completed_at
                    )

                    for direction, wallet in data["wallet_changes"]:
                        apply_wallet_change(
                            cur,
                            run_id,
                            wallet,
                            direction,
                            amount_paisa,
                            tx_id,
                            completed_at
                        )
                        outbox_total += 1

                    if data["agent_change"]:
                        apply_agent_change(
                            cur,
                            run_id,
                            agent,
                            data["agent_change"],
                            amount_paisa,
                            tx_id,
                            completed_at
                        )
                        outbox_total += 1

                    if data["funding_provider"]:
                        data["funding_provider"]["balance_paisa"] -= amount_paisa

                # One final transaction event for every outcome.
                tx_event_type = {
                    "SUCCESS": "TRANSACTION_COMPLETED",
                    "FAILED": "TRANSACTION_FAILED",
                    "PENDING": "TRANSACTION_PENDING",
                }[status]

                tx_payload = {
                    "transaction_id": str(tx_id),
                    "transaction_ref": tx_ref,
                    "transaction_type": tx_type,
                    "transaction_status": status,
                    "amount_paisa": amount_paisa,
                    "currency": "BDT",
                    "sender_wallet_id": (
                        str(sender["wallet_id"]) if sender else None
                    ),
                    "receiver_wallet_id": (
                        str(receiver["wallet_id"]) if receiver else None
                    ),
                    "agent_outlet_id": (
                        str(agent["outlet_id"]) if agent else None
                    ),
                    "merchant_id": (
                        str(merchant["merchant_id"]) if merchant else None
                    ),
                    "terminal_id": (
                        str(terminal["terminal_id"]) if terminal else None
                    ),
                    "service_provider_id": (
                        str(provider["provider_id"]) if provider else None
                    ),
                    "channel": channel,
                    "initiated_at": initiated_at.isoformat(),
                    "completed_at": (
                        completed_at.isoformat()
                        if completed_at else None
                    ),
                    "related_transaction_id": None,
                }

                insert_outbox(
                    cur,
                    run_id,
                    TOPICS["TRANSACTION"],
                    tx_event_type,
                    "TRANSACTION",
                    tx_id,
                    str(tx_id),
                    tx_payload,
                    event_time,
                    tx_id,
                )
                outbox_total += 1

                # Merchant operational event, including failed results.
                if tx_type == "MERCHANT_PAYMENT":
                    merchant_payload = {
                        "merchant_id": str(merchant["merchant_id"]),
                        "terminal_id": (
                            str(terminal["terminal_id"])
                            if terminal else None
                        ),
                        "transaction_id": str(tx_id),
                        "event_category": "PAYMENT_RESULT",
                        "payment_status": status,
                        "amount_paisa": amount_paisa,
                        "fee_paisa": 0,
                        "currency": "BDT",
                    }

                    insert_outbox(
                        cur,
                        run_id,
                        TOPICS["MERCHANT"],
                        "MERCHANT_PAYMENT_RESULT",
                        "MERCHANT",
                        merchant["merchant_id"],
                        str(merchant["merchant_id"]),
                        merchant_payload,
                        event_time,
                        tx_id,
                    )
                    outbox_total += 1

                actual_types[tx_type] += 1
                actual_statuses[status] += 1

                if index % 100 == 0:
                    print(
                        f"Prepared {index}/{total} transactions..."
                    )

            # Mark this run complete in the same database transaction.
            cur.execute("""
                UPDATE integration.source_runs
                SET
                    run_status = 'COMPLETED',
                    created_count = %s,
                    failed_count = %s,
                    completed_at = NOW(),
                    metadata = metadata || %s
                WHERE run_id = %s
                  AND run_status = 'RUNNING'
            """, (
                total,
                actual_statuses["FAILED"],
                Jsonb({
                    "transaction_types": dict(actual_types),
                    "transaction_statuses": dict(actual_statuses),
                    "outbox_events_created": outbox_total,
                }),
                run_id,
            ))

            if cur.rowcount != 1:
                raise RuntimeError("Source run completion update failed.")

    # Context manager commits the complete financial batch here.
    return actual_types, actual_statuses, outbox_total


def main():
    parser = argparse.ArgumentParser(
        description="BanglaPay manual financial transaction batch"
    )

    parser.add_argument(
        "--transactions",
        type=int,
        default=int(RULES["transaction_count"]["default"])
    )

    parser.add_argument(
        "--run-id",
        type=uuid.UUID,
        default=None
    )

    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    maximum = int(RULES["transaction_count"]["maximum"])

    if not 1 <= args.transactions <= maximum:
        parser.error(f"Transactions must be between 1 and {maximum}.")

    run_id = args.run_id or uuid.uuid4()
    seed = run_id.int % (2**32)

    type_counts = allocate_counts(
        args.transactions,
        TX_WEIGHTS
    )

    status_counts = allocate_counts(
        args.transactions,
        STATUS_WEIGHTS
    )

    print("\n========== BANGLAPAY TRANSACTION BATCH ==========")
    print("Run ID:", run_id)
    print("Requested transactions:", args.transactions)

    print("\nTransaction allocation:")
    for key, value in type_counts.items():
        print(f"  {key}: {value}")

    print("\nStatus allocation:")
    for key, value in status_counts.items():
        print(f"  {key}: {value}")

    if args.dry_run:
        print("\nDRY RUN: No database writes.")
        return 0

    try:
        register_run(run_id, args.transactions, seed)

        types, statuses, outbox_count = execute_batch(
            run_id,
            args.transactions,
            seed
        )

        print("\n========== BATCH COMMITTED ==========")
        print("Run ID:", run_id)
        print("Transactions:", sum(types.values()))
        print("Successful:", statuses["SUCCESS"])
        print("Failed:", statuses["FAILED"])
        print("Pending:", statuses["PENDING"])
        print("Outbox events:", outbox_count)

        print("\nTransaction type results:")
        for key, count in types.items():
            print(f"  {key}: {count}")

        print("\nKafka publication is NOT started.")
        print("Outbox events are waiting in PostgreSQL.")

        return 0

    except Exception as exc:
        try:
            mark_run_failed(run_id, exc)
        except Exception as tracking_error:
            print("WARNING: Could not update source run status:",
                  tracking_error)

        print(
            f"\nBATCH FAILED: {type(exc).__name__}: {exc}"
        )
        print("Financial batch rolled back.")
        print("Run ID:", run_id)

        return 1


if __name__ == "__main__":
    raise SystemExit(main())
