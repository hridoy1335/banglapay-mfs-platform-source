#!/usr/bin/env python3
"""BanglaPay single entry point for manual batch and timed micro-batch streaming.

This orchestrator reuses the existing project scripts. It does not seed master
/reference data; run scripts/seed_master_data.py separately only when needed.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path
from typing import Sequence

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
TRANSACTION_SCRIPT = SCRIPTS_DIR / "run_transaction_batch.py"
OPERATIONAL_SCRIPT = SCRIPTS_DIR / "run_operational_batch.py"


def run_step(label: str, command: Sequence[str]) -> None:
    print(f"\n{'=' * 16} {label} {'=' * 16}", flush=True)
    print("$", " ".join(command), flush=True)
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def run_transactions(count: int) -> None:
    run_step(
        f"Neon transaction batch: {count}",
        [sys.executable, str(TRANSACTION_SCRIPT), "--transactions", str(count)],
    )


def run_operational(
    sessions: int,
    activities: int,
    fraud_cases: int,
    support_cases: int,
    merchant_events: int,
) -> None:
    run_step(
        "MongoDB operational batch",
        [
            sys.executable,
            str(OPERATIONAL_SCRIPT),
            "--sessions", str(sessions),
            "--activities", str(activities),
            "--fraud-cases", str(fraud_cases),
            "--support-cases", str(support_cases),
            "--merchant-events", str(merchant_events),
        ],
    )


def publish_outbox(batch_size: int) -> None:
    run_step(
        f"Kafka Outbox publish (up to {batch_size} events)",
        [
            sys.executable,
            "-m",
            "src.publishers.outbox_publisher",
            "--batch-size", str(batch_size),
        ],
    )


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return parsed


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run BanglaPay source generation and publish Outbox events."
    )
    parser.add_argument(
        "--mode", choices=("batch", "streaming"), default="batch",
        help="batch runs once; streaming repeats micro-batches for a duration (default: batch)",
    )

    # Batch-mode volumes
    parser.add_argument("--transactions", type=positive_int, default=100)
    parser.add_argument("--sessions", type=non_negative_int, default=50)
    parser.add_argument("--activities", type=non_negative_int, default=500)
    parser.add_argument("--fraud-cases", type=non_negative_int, default=10)
    parser.add_argument("--support-cases", type=non_negative_int, default=20)
    parser.add_argument("--merchant-events", type=non_negative_int, default=100)

    # Kafka Outbox publisher limit per invocation
    parser.add_argument("--publish-batch-size", type=positive_int, default=10000)

    # Streaming-mode controls. --rate means target transactions per second.
    parser.add_argument(
        "--duration", type=positive_float, default=30,
        help="streaming duration in minutes (default: 30)",
    )
    parser.add_argument(
        "--rate", type=positive_float, default=5,
        help="target transaction generation rate per second (default: 5)",
    )
    parser.add_argument(
        "--interval-seconds", type=positive_float, default=5,
        help="length of each streaming micro-batch cycle in seconds (default: 5)",
    )
    parser.add_argument("--sessions-per-cycle", type=non_negative_int, default=1)
    parser.add_argument("--activities-per-cycle", type=non_negative_int, default=25)
    parser.add_argument("--fraud-cases-per-cycle", type=non_negative_int, default=0)
    parser.add_argument("--support-cases-per-cycle", type=non_negative_int, default=0)
    parser.add_argument("--merchant-events-per-cycle", type=non_negative_int, default=5)
    return parser


def run_batch(args: argparse.Namespace) -> None:
    if args.transactions > 5000:
        raise ValueError(
            "The existing transaction batch generator supports at most 5000 records per run."
        )

    # Generate financial records first so the Mongo operational generator can
    # use the customers/transactions already present in PostgreSQL.
    run_transactions(args.transactions)
    run_operational(
        args.sessions,
        args.activities,
        args.fraud_cases,
        args.support_cases,
        args.merchant_events,
    )
    publish_outbox(args.publish_batch_size)
    print("\nBanglaPay batch run completed.", flush=True)


def run_streaming(args: argparse.Namespace) -> None:
    deadline = time.monotonic() + args.duration * 60
    cycle = 0
    transaction_batch_max = 5000

    print(
        f"Starting timed micro-batch streaming for {args.duration:g} minutes; "
        f"target rate={args.rate:g} transactions/second; "
        f"cycle={args.interval_seconds:g} seconds. Press Ctrl+C to stop.",
        flush=True,
    )

    while time.monotonic() < deadline:
        cycle += 1
        cycle_started = time.monotonic()
        remaining_seconds = max(0.1, deadline - cycle_started)
        cycle_window = min(args.interval_seconds, remaining_seconds)
        transaction_count = max(1, round(args.rate * cycle_window))
        if transaction_count > transaction_batch_max:
            raise ValueError(
                f"This cycle requests {transaction_count} transactions; "
                f"reduce --rate or --interval-seconds so each micro-batch is <= {transaction_batch_max}."
            )

        print(f"\n\n######## STREAMING CYCLE {cycle} ########", flush=True)
        run_transactions(transaction_count)
        run_operational(
            args.sessions_per_cycle,
            args.activities_per_cycle,
            args.fraud_cases_per_cycle,
            args.support_cases_per_cycle,
            args.merchant_events_per_cycle,
        )
        publish_outbox(args.publish_batch_size)

        elapsed = time.monotonic() - cycle_started
        sleep_for = args.interval_seconds - elapsed
        if sleep_for > 0 and time.monotonic() < deadline:
            time.sleep(min(sleep_for, max(0, deadline - time.monotonic())))

    print(
        f"\nTimed streaming finished. Completed {cycle} micro-batch cycles.",
        flush=True,
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    required_files = (TRANSACTION_SCRIPT, OPERATIONAL_SCRIPT)
    missing = [str(path.relative_to(PROJECT_ROOT)) for path in required_files if not path.is_file()]
    if missing:
        parser.error("Required existing project scripts not found: " + ", ".join(missing))

    try:
        if args.mode == "batch":
            run_batch(args)
        else:
            run_streaming(args)
    except KeyboardInterrupt:
        print("\nStopped by user. Any committed PostgreSQL batches remain committed; "
              "unpublished Outbox events can be published in a later run.", flush=True)
        return 130
    except subprocess.CalledProcessError as exc:
        print(
            f"\nA child script failed (exit code {exc.returncode}). "
            "The runner stopped; check the output above before retrying.",
            file=sys.stderr,
            flush=True,
        )
        return exc.returncode or 1
    except Exception as exc:
        print(f"\nRun failed: {exc}", file=sys.stderr, flush=True)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
