from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from uuid import UUID, NAMESPACE_URL, uuid5


def stable_id(run_id: UUID, collection: str, number: int) -> str:
    """Generate repeatable document IDs for a given run."""
    return str(
        uuid5(
            NAMESPACE_URL,
            f"banglapay:{run_id}:{collection}:{number}",
        )
    )


def value(row: dict, *keys: str, default=None):
    for key in keys:
        result = row.get(key)
        if result is not None:
            return result
    return default


def as_string(value_) -> str | None:
    return str(value_) if value_ is not None else None


def build_operational_documents(
    *,
    run_id: UUID,
    customers: list[dict],
    transactions: list[dict],
    terminals: list[dict],
    device_profiles: list[dict],
    session_count: int = 300,
    activity_count: int = 1000,
    fraud_count: int = 10,
    support_count: int = 20,
    merchant_event_count: int = 100,
) -> dict[str, list[dict]]:

    rng = random.Random(run_id.int % (2**32))
    now = datetime.now(timezone.utc)

    if not customers:
        raise ValueError("No customer/wallet records found in PostgreSQL.")

    if merchant_event_count > 0 and not terminals:
        raise ValueError("No merchant terminals found in PostgreSQL.")

    customer_ids = [
        as_string(row["customer_id"])
        for row in customers
    ]

    devices_by_customer: dict[str, list[dict]] = {}

    for device in device_profiles:
        customer_id = as_string(
            value(device, "customer_id", "customerId", "customer_uuid")
        )
        if customer_id:
            devices_by_customer.setdefault(customer_id, []).append(device)

    transactions_by_customer: dict[str, list[dict]] = {}

    for tx in transactions:
        customer_id = as_string(
            value(
                tx,
                "sender_customer_id",
                "receiver_customer_id",
                "customer_id",
            )
        )

        if customer_id:
            transactions_by_customer.setdefault(
                customer_id, []
            ).append(tx)

    # ---------------------------------------------------------
    # 1. APP SESSIONS
    # ---------------------------------------------------------

    sessions = []

    for i in range(session_count):
        customer = rng.choice(customers)
        customer_id = as_string(customer["customer_id"])
        wallet_id = as_string(customer["wallet_id"])

        device_options = devices_by_customer.get(customer_id, [])
        device = rng.choice(device_options) if device_options else None

        started_at = now - timedelta(
            seconds=rng.randint(60, 30 * 24 * 3600)
        )

        duration = rng.randint(20, 2400)
        ended_at = started_at + timedelta(seconds=duration)

        is_open = rng.random() < 0.04

        session_id = stable_id(run_id, "app_sessions", i)

        session = {
            "_id": session_id,
            "run_id": str(run_id),
            "session_id": session_id,
            "customer_id": customer_id,
            "wallet_id": wallet_id,
            "device_profile_id": (
                as_string(device.get("_id")) if device else None
            ),
            "device_type": (
                value(device, "device_type", "platform", "os_family")
                if device else "UNKNOWN"
            ),
            "channel": rng.choices(
                ["ANDROID_APP", "IOS_APP", "WEB_APP"],
                weights=[75, 20, 5],
                k=1,
            )[0],
            "session_status": "ACTIVE" if is_open else "CLOSED",
            "started_at": started_at,
            "ended_at": None if is_open else ended_at,
            "duration_seconds": duration,
            "ip_token": f"SYNTHETIC-IP-{rng.randint(100000, 999999)}",
            "location": {
                "country": "BD",
                "district": rng.choice([
                    "Dhaka", "Gazipur", "Manikganj",
                    "Chattogram", "Cumilla", "Rajshahi",
                ]),
            },
            "created_at": now,
        }

        sessions.append(session)

    # ---------------------------------------------------------
    # 2. CUSTOMER ACTIVITIES
    # ---------------------------------------------------------

    activity_types = [
        "APP_OPEN",
        "HOME_VIEW",
        "BALANCE_VIEW",
        "TRANSACTION_HISTORY_VIEW",
        "BENEFICIARY_LIST_VIEW",
        "QR_SCAN",
        "HELP_CENTER_OPEN",
        "PROFILE_VIEW",
        "NOTIFICATION_VIEW",
        "LOGOUT",
    ]

    activities = []

    for i in range(activity_count):
        session = rng.choice(sessions)

        started_at = session["started_at"]
        duration = session["duration_seconds"]

        occurred_at = started_at + timedelta(
            seconds=rng.randint(0, duration)
        )

        activity_type = rng.choice(activity_types)

        activities.append({
            "_id": stable_id(run_id, "customer_activities", i),
            "run_id": str(run_id),
            "customer_id": session["customer_id"],
            "wallet_id": session["wallet_id"],
            "session_id": session["session_id"],
            "activity_type": activity_type,
            "screen": activity_type.replace("_", "/").lower(),
            "occurred_at": occurred_at,
            "metadata": {
                "app_version": "1.0.0",
                "platform": session["channel"],
                "simulation": True,
            },
            "created_at": now,
        })

    # ---------------------------------------------------------
    # 3. FRAUD / RISK SIMULATION CASES
    # ---------------------------------------------------------

    fraud_cases = []

    selected_transactions = (
        rng.sample(transactions, min(fraud_count, len(transactions)))
        if fraud_count > 0 and transactions else []
    )

    risk_reasons = [
        "VELOCITY_SPIKE",
        "NEW_DEVICE_ACTIVITY",
        "UNUSUAL_TRANSACTION_PATTERN",
        "MULTIPLE_FAILED_ATTEMPTS",
        "UNUSUAL_LOGIN_PATTERN",
    ]

    for i, tx in enumerate(selected_transactions):
        customer_id = as_string(
            value(
                tx,
                "sender_customer_id",
                "receiver_customer_id",
                "customer_id",
            )
        )

        if not customer_id:
            customer_id = rng.choice(customer_ids)

        fraud_cases.append({
            "_id": stable_id(run_id, "fraud_cases", i),
            "run_id": str(run_id),
            "case_ref": f"RISK-{str(run_id)[:8]}-{i + 1:04d}",
            "transaction_id": as_string(
                value(tx, "transaction_id")
            ),
            "customer_id": customer_id,
            "risk_type": rng.choice(risk_reasons),
            "risk_score": rng.randint(65, 99),
            "risk_level": rng.choice(["HIGH", "CRITICAL"]),
            "case_status": rng.choice([
                "OPEN", "UNDER_REVIEW", "CLOSED_FALSE_POSITIVE"
            ]),
            "signals": [
                "SYNTHETIC_RISK_SIGNAL",
                "RULE_BASED_SIMULATION",
            ],
            "simulation_label": "SYNTHETIC_RISK_CASE",
            "detected_at": now - timedelta(
                minutes=rng.randint(1, 60 * 24 * 7)
            ),
            "created_at": now,
        })

    # ---------------------------------------------------------
    # 4. CUSTOMER SUPPORT CASES
    # ---------------------------------------------------------

    support_types = [
        "TRANSACTION_STATUS",
        "WALLET_BALANCE",
        "ACCOUNT_ACCESS",
        "KYC_QUERY",
        "MERCHANT_PAYMENT",
    ]

    support_cases = []

    for i in range(support_count):
        customer = rng.choice(customers)
        customer_id = as_string(customer["customer_id"])

        related = transactions_by_customer.get(customer_id, [])
        tx = rng.choice(related) if related else None

        opened_at = now - timedelta(
            seconds=rng.randint(60, 30 * 24 * 3600)
        )

        support_cases.append({
            "_id": stable_id(run_id, "support_cases", i),
            "run_id": str(run_id),
            "case_ref": f"SUP-{str(run_id)[:8]}-{i + 1:04d}",
            "customer_id": customer_id,
            "wallet_id": as_string(customer["wallet_id"]),
            "transaction_id": (
                as_string(tx.get("transaction_id")) if tx else None
            ),
            "category": rng.choice(support_types),
            "priority": rng.choice(["LOW", "MEDIUM", "HIGH"]),
            "status": rng.choice([
                "OPEN", "IN_PROGRESS", "RESOLVED"
            ]),
            "description": "Synthetic customer support scenario.",
            "opened_at": opened_at,
            "created_at": now,
            "simulation_label": "SYNTHETIC_SUPPORT_CASE",
        })

    # ---------------------------------------------------------
    # 5. MERCHANT DEVICE EVENTS
    # ---------------------------------------------------------

    device_event_types = [
        "TERMINAL_HEARTBEAT",
        "DEVICE_OFFLINE",
        "QR_SCAN_FAILED",
        "FIRMWARE_UPDATE",
        "PRINTER_ERROR",
    ]

    merchant_events = []

    for i in range(merchant_event_count):
        terminal = rng.choice(terminals)

        terminal_id = value(
            terminal, "terminal_id", "merchant_terminal_id", "id"
        )
        merchant_id = value(terminal, "merchant_id")

        if terminal_id is None or merchant_id is None:
            raise ValueError(
                "Merchant terminal records must contain terminal_id "
                "and merchant_id. Inspect merchant.merchant_terminals."
            )

        event_type = rng.choice(device_event_types)

        merchant_events.append({
            "_id": stable_id(run_id, "merchant_device_events", i),
            "run_id": str(run_id),
            "merchant_id": as_string(merchant_id),
            "terminal_id": as_string(terminal_id),
            "event_type": event_type,
            "event_status": (
                "WARNING" if event_type in {
                    "DEVICE_OFFLINE",
                    "QR_SCAN_FAILED",
                    "PRINTER_ERROR",
                } else "NORMAL"
            ),
            "occurred_at": now - timedelta(
                seconds=rng.randint(60, 30 * 24 * 3600)
            ),
            "metadata": {
                "firmware_version": "1.0.0",
                "simulation": True,
            },
            "created_at": now,
        })

    return {
        "app_sessions": sessions,
        "customer_activities": activities,
        "fraud_cases": fraud_cases,
        "support_cases": support_cases,
        "merchant_device_events": merchant_events,
    }