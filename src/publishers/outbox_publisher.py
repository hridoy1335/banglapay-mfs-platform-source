"""Manual PostgreSQL transactional-Outbox to Confluent Kafka publisher."""
from __future__ import annotations

import argparse
import math
import os
from datetime import datetime
from typing import Any

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

from src.publishers.kafka_producer import (
    ROOT_DIR,
    build_avro_record,
    create_kafka_clients,
    load_project_env,
    serialize_event,
)


def database_dsn() -> str:
    load_project_env()
    dsn = os.getenv("NEON_DATABASE_URL")
    if not dsn:
        raise RuntimeError("Missing required .env setting: NEON_DATABASE_URL")
    return dsn


def claim_events(
    dsn: str,
    batch_size: int,
    lease_seconds: int,
    topic: str | None,
) -> list[dict[str, Any]]:
    """Claim a small batch quickly; never keep a DB lock during Kafka I/O."""
    query = """
        WITH candidates AS (
            SELECT event_id
            FROM integration.outbox_events
            WHERE (
                (publish_status = 'PENDING'
                 AND COALESCE(available_at, '-infinity'::timestamptz) <= clock_timestamp())
                OR
                (publish_status = 'PROCESSING'
                 AND (locked_at IS NULL OR locked_at <
                      clock_timestamp() - (%s * INTERVAL '1 second')))
            )
              AND (%s::text IS NULL OR topic_name = %s)
            ORDER BY created_at, event_id
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        UPDATE integration.outbox_events AS outbox
        SET publish_status = 'PROCESSING',
            locked_at = clock_timestamp(),
            attempt_count = outbox.attempt_count + 1
        FROM candidates
        WHERE outbox.event_id = candidates.event_id
        RETURNING
            outbox.event_id::text AS event_id,
            outbox.event_type,
            outbox.topic_name,
            outbox.aggregate_type,
            outbox.aggregate_id::text AS aggregate_id,
            outbox.event_payload,
            outbox.run_id::text AS run_id,
            outbox.attempt_count,
            outbox.locked_at
    """

    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (lease_seconds, topic, topic, batch_size))
            rows = cursor.fetchall()
    return [dict(row) for row in rows]


def _record_result(
    producer: Any,
    rows: list[dict[str, Any]],
    serializers: dict[str, Any],
    schemas: dict[str, dict[str, Any]],
    flush_timeout: float,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}

    for row in rows:
        event_id = row["event_id"]
        topic = row["topic_name"]
        try:
            record = build_avro_record(row, schemas)
            value = serialize_event(topic, record, serializers)
            key = record.get("aggregate_id") or event_id
            headers = [
                ("event_id", event_id.encode("utf-8")),
                ("event_type", str(record["event_type"]).encode("utf-8")),
            ]

            def delivery_callback(error: Any, message: Any, eid: str = event_id) -> None:
                if error is not None:
                    results[eid] = {"ok": False, "error": str(error)}
                else:
                    results[eid] = {
                        "ok": True,
                        "partition": message.partition(),
                        "offset": message.offset(),
                    }

            producer.produce(
                topic=topic,
                key=key,
                value=value,
                headers=headers,
                on_delivery=delivery_callback,
            )
            producer.poll(0)
        except Exception as exc:  # validation, serialization, or local queue errors
            results[event_id] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    outstanding = producer.flush(flush_timeout)
    if outstanding:
        for row in rows:
            event_id = row["event_id"]
            if event_id not in results:
                results[event_id] = {
                    "ok": False,
                    "error": f"Kafka flush timed out; {outstanding} message(s) still queued",
                }

    # Any event without a delivery callback is not safe to mark as published.
    for row in rows:
        event_id = row["event_id"]
        if event_id not in results:
            results[event_id] = {
                "ok": False,
                "error": "No Kafka delivery callback received; retry required",
            }
    return results


def retry_delay_seconds(attempt_count: int, base_seconds: int, max_seconds: int) -> int:
    exponent = max(0, attempt_count - 1)
    return min(max_seconds, base_seconds * (2 ** exponent))


def persist_results(
    dsn: str,
    rows: list[dict[str, Any]],
    results: dict[str, dict[str, Any]],
    max_attempts: int,
    base_backoff_seconds: int,
    max_backoff_seconds: int,
) -> tuple[int, int, int]:
    """Persist statuses only for the worker that still owns the lock timestamp."""
    published = 0
    retried = 0
    failed = 0

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            for row in rows:
                event_id = row["event_id"]
                result = results[event_id]

                if result["ok"]:
                    cursor.execute(
                        """
                        UPDATE integration.outbox_events
                        SET publish_status = 'PUBLISHED',
                            published_at = clock_timestamp(),
                            locked_at = NULL,
                            last_error = NULL
                        WHERE event_id = %s
                          AND publish_status = 'PROCESSING'
                          AND locked_at = %s
                        """,
                        (event_id, row["locked_at"]),
                    )
                    if cursor.rowcount == 1:
                        published += 1
                        print(
                            f"PUBLISHED event_id={event_id} "
                            f"topic={row['topic_name']} "
                            f"partition={result.get('partition')} offset={result.get('offset')}"
                        )
                    else:
                        print(f"LOCK LOST; not updating event_id={event_id}")
                    continue

                attempt_count = int(row["attempt_count"])
                terminal = attempt_count >= max_attempts
                new_status = "FAILED" if terminal else "PENDING"
                delay = retry_delay_seconds(
                    attempt_count, base_backoff_seconds, max_backoff_seconds
                )
                error_text = str(result.get("error", "Unknown Kafka delivery error"))[:4000]

                cursor.execute(
                    """
                    UPDATE integration.outbox_events
                    SET publish_status = %s,
                        available_at = CASE
                            WHEN %s = 'PENDING'
                            THEN clock_timestamp() + (%s * INTERVAL '1 second')
                            ELSE available_at
                        END,
                        locked_at = NULL,
                        last_error = %s
                    WHERE event_id = %s
                      AND publish_status = 'PROCESSING'
                      AND locked_at = %s
                    """,
                    (
                        new_status,
                        new_status,
                        delay,
                        error_text,
                        event_id,
                        row["locked_at"],
                    ),
                )
                if cursor.rowcount == 1:
                    if terminal:
                        failed += 1
                        print(f"FAILED event_id={event_id}: {error_text}")
                    else:
                        retried += 1
                        print(
                            f"RETRY event_id={event_id} in={delay}s "
                            f"attempt={attempt_count}/{max_attempts}: {error_text}"
                        )
                else:
                    print(f"LOCK LOST; not updating event_id={event_id}")

    return published, retried, failed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish one manually-triggered batch from PostgreSQL Outbox to Kafka."
    )
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--topic", default=None, help="Optional single-topic pilot filter")
    parser.add_argument("--lease-seconds", type=int, default=300)
    parser.add_argument("--max-attempts", type=int, default=8)
    parser.add_argument("--base-backoff-seconds", type=int, default=5)
    parser.add_argument("--max-backoff-seconds", type=int, default=300)
    parser.add_argument("--flush-timeout", type=float, default=30.0)
    args = parser.parse_args()

    if not 1 <= args.batch_size <= 1000:
        parser.error("--batch-size must be between 1 and 1000")
    if args.lease_seconds < 30:
        parser.error("--lease-seconds must be at least 30")
    if args.max_attempts < 1:
        parser.error("--max-attempts must be at least 1")
    if args.base_backoff_seconds < 1 or args.max_backoff_seconds < 1:
        parser.error("backoff values must be positive")
    if args.topic and args.topic not in {
        "mfs.agent.events.v1",
        "mfs.customer.activity.v1",
        "mfs.merchant.events.v1",
        "mfs.risk.events.v1",
        "mfs.settlement.events.v1",
        "mfs.transaction.events.v1",
        "mfs.wallet.events.v1",
    }:
        parser.error("--topic is not one of the seven BanglaPay topics")
    return args


def main() -> int:
    args = parse_args()
    load_project_env()
    dsn = database_dsn()

    # Instantiate/validate client configuration before claiming rows.
    producer, serializers, schemas = create_kafka_clients()

    rows = claim_events(
        dsn=dsn,
        batch_size=args.batch_size,
        lease_seconds=args.lease_seconds,
        topic=args.topic,
    )
    if not rows:
        print("No eligible Outbox events found. No changes made.")
        producer.flush(5)
        return 0

    print(f"Claimed {len(rows)} event(s) for one manual batch.")
    for row in rows:
        print(
            f"  event_id={row['event_id']} topic={row['topic_name']} "
            f"type={row['event_type']} attempt={row['attempt_count']}"
        )

    results = _record_result(
        producer=producer,
        rows=rows,
        serializers=serializers,
        schemas=schemas,
        flush_timeout=args.flush_timeout,
    )
    published, retried, failed = persist_results(
        dsn=dsn,
        rows=rows,
        results=results,
        max_attempts=args.max_attempts,
        base_backoff_seconds=args.base_backoff_seconds,
        max_backoff_seconds=args.max_backoff_seconds,
    )

    print(
        "\nBatch summary: "
        f"claimed={len(rows)}, published={published}, retried={retried}, failed={failed}"
    )
    print("Delivery semantics: at-least-once; downstream consumers must deduplicate by event_id.")
    return 0 if retried == 0 and failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
