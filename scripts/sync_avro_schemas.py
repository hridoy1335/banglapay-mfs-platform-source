#!/usr/bin/env python3
"""Download the latest registered BanglaPay Avro schemas into schemas/avro."""
from __future__ import annotations

import json
import os
from pathlib import Path

from dotenv import load_dotenv
from confluent_kafka.schema_registry import SchemaRegistryClient

ROOT_DIR = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT_DIR / "schemas" / "avro"

# Registry subjects were confirmed from the user's Schema Registry output.
SUBJECT_FILES = {
    "mfs.agent.events.v1-value": "agent.avsc",
    "mfs.customer.activity.v1-value": "customer_activity.avsc",
    "mfs.merchant.events.v1-value": "merchant.avsc",
    "mfs.risk.events.v1-value": "risk.avsc",
    "mfs.settlement.events.v1-value": "settlement.avsc",
    "mfs.transaction.events.v1-value": "transaction.avsc",
    "mfs.wallet.events.v1-value": "wallet.avsc",
}


def main() -> int:
    load_dotenv(dotenv_path=ROOT_DIR / ".env")

    url = os.getenv("SCHEMA_REGISTRY_URL")
    api_key = os.getenv("SCHEMA_REGISTRY_API_KEY")
    api_secret = os.getenv("SCHEMA_REGISTRY_API_SECRET")
    missing = [
        name
        for name, value in (
            ("SCHEMA_REGISTRY_URL", url),
            ("SCHEMA_REGISTRY_API_KEY", api_key),
            ("SCHEMA_REGISTRY_API_SECRET", api_secret),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing .env settings: {', '.join(missing)}")

    client = SchemaRegistryClient(
        {
            "url": url.rstrip("/"),
            "basic.auth.user.info": f"{api_key}:{api_secret}",
        }
    )
    SCHEMA_DIR.mkdir(parents=True, exist_ok=True)

    for subject, filename in SUBJECT_FILES.items():
        latest = client.get_latest_version(subject)
        schema_text = latest.schema.schema_str
        # Parse and pretty-print to make the local .avsc files human-readable.
        schema_document = json.loads(schema_text)
        destination = SCHEMA_DIR / filename
        destination.write_text(
            json.dumps(schema_document, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(
            f"Synced {subject} -> {destination.relative_to(ROOT_DIR)} "
            f"(version={latest.version}, schema_id={latest.schema_id})"
        )

    print("\nSchema sync completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
