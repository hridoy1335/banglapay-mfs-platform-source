"""Confluent Kafka and Avro client helpers for BanglaPay Outbox publishing."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[2]

TOPIC_SCHEMA_FILES = {
    "mfs.agent.events.v1": "agent.avsc",
    "mfs.customer.activity.v1": "customer_activity.avsc",
    "mfs.merchant.events.v1": "merchant.avsc",
    "mfs.risk.events.v1": "risk.avsc",
    "mfs.settlement.events.v1": "settlement.avsc",
    "mfs.transaction.events.v1": "transaction.avsc",
    "mfs.wallet.events.v1": "wallet.avsc",
}


def load_project_env() -> None:
    """Load this repository's .env explicitly (works with python -m/heredoc)."""
    load_dotenv(dotenv_path=ROOT_DIR / ".env")


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required .env setting: {name}")
    return value


def create_kafka_clients() -> tuple[Producer, dict[str, AvroSerializer], dict[str, dict[str, Any]]]:
    """Create one Kafka producer and one Avro serializer per topic."""
    load_project_env()

    producer = Producer(
        {
            "bootstrap.servers": _required_env("KAFKA_BOOTSTRAP_SERVERS"),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": _required_env("KAFKA_API_KEY"),
            "sasl.password": _required_env("KAFKA_API_SECRET"),
            "client.id": "banglapay-outbox-publisher",
            "acks": "all",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
            "compression.type": "gzip",
            "message.timeout.ms": 30000,
        }
    )

    registry = SchemaRegistryClient(
        {
            "url": _required_env("SCHEMA_REGISTRY_URL").rstrip("/"),
            "basic.auth.user.info": (
                f"{_required_env('SCHEMA_REGISTRY_API_KEY')}:"
                f"{_required_env('SCHEMA_REGISTRY_API_SECRET')}"
            ),
        }
    )

    serializers: dict[str, AvroSerializer] = {}
    schemas: dict[str, dict[str, Any]] = {}
    schema_dir = ROOT_DIR / "schemas" / "avro"

    for topic, filename in TOPIC_SCHEMA_FILES.items():
        schema_path = schema_dir / filename
        if not schema_path.is_file() or schema_path.stat().st_size == 0:
            raise RuntimeError(
                f"Avro schema missing/empty: {schema_path}. "
                "Run: python -m scripts.sync_avro_schemas"
            )

        schema_text = schema_path.read_text(encoding="utf-8")
        schema_document = json.loads(schema_text)
        # Registration is deliberately disabled: the seven subjects already exist.
        serializers[topic] = AvroSerializer(
            registry,
            schema_text,
            conf={"auto.register.schemas": False},
        )
        schemas[topic] = schema_document

    return producer, serializers, schemas


def parse_json_payload(value: Any) -> dict[str, Any]:
    """Handle psycopg JSONB dicts as well as string/bytes representations."""
    if isinstance(value, dict):
        return value
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    raise TypeError(f"event_payload must be a JSON object, got {type(value).__name__}")


def _timestamp_value(value: Any) -> datetime:
    """Convert ISO-8601 or epoch-millisecond values to an aware UTC datetime."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        parsed = datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    else:
        raise TypeError(f"Unsupported timestamp value: {value!r}")

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_for_avro(value: Any, schema: Any, path: str = "value") -> Any:
    """Normalize records to the local Avro schema, including timestamp logical types."""
    if isinstance(schema, list):
        if value is None:
            if "null" in schema:
                return None
            raise ValueError(f"{path}: null is not allowed by schema union")
        non_null = [branch for branch in schema if branch != "null"]
        if len(non_null) != 1:
            raise ValueError(f"{path}: unsupported multi-branch Avro union")
        return normalize_for_avro(value, non_null[0], path)

    if isinstance(schema, dict):
        if schema.get("logicalType") == "timestamp-millis":
            return _timestamp_value(value)

        schema_type = schema.get("type")
        if schema_type == "record":
            if not isinstance(value, dict):
                raise TypeError(f"{path}: expected object for Avro record")
            field_names = {field["name"] for field in schema["fields"]}
            extras = set(value) - field_names
            if extras:
                raise ValueError(f"{path}: fields not present in Avro schema: {sorted(extras)}")

            normalized: dict[str, Any] = {}
            for field in schema["fields"]:
                name = field["name"]
                if name in value:
                    field_value = value[name]
                elif "default" in field:
                    field_value = field["default"]
                else:
                    raise ValueError(f"{path}.{name}: required Avro field is missing")
                normalized[name] = normalize_for_avro(
                    field_value, field["type"], f"{path}.{name}"
                )
            return normalized

        if schema_type == "array":
            if not isinstance(value, list):
                raise TypeError(f"{path}: expected array")
            return [
                normalize_for_avro(item, schema["items"], f"{path}[{index}]")
                for index, item in enumerate(value)
            ]

        if schema_type == "map":
            if not isinstance(value, dict):
                raise TypeError(f"{path}: expected map/object")
            return {
                str(key): normalize_for_avro(item, schema["values"], f"{path}.{key}")
                for key, item in value.items()
            }

        return normalize_for_avro(value, schema_type, path)

    if schema == "null":
        if value is not None:
            raise TypeError(f"{path}: expected null")
        return None
    if schema == "string":
        if not isinstance(value, str):
            raise TypeError(f"{path}: expected string, got {type(value).__name__}")
        return value
    if schema in ("int", "long"):
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{path}: expected integer, got {type(value).__name__}")
        return value
    if schema == "boolean":
        if not isinstance(value, bool):
            raise TypeError(f"{path}: expected boolean")
        return value
    if schema in ("float", "double"):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"{path}: expected number")
        return value

    # Avro enum/fixed are not present in the currently registered seven schemas.
    return value


def build_avro_record(row: dict[str, Any], schemas: dict[str, dict[str, Any]]) -> dict[str, Any]:
    topic = row["topic_name"]
    if topic not in schemas:
        raise ValueError(f"No registered topic/schema mapping for topic: {topic}")

    record = parse_json_payload(row["event_payload"])
    # The Outbox table is authoritative for these identifiers if an envelope omits one.
    record.setdefault("event_id", str(row["event_id"]))
    record.setdefault("event_type", row["event_type"])
    record.setdefault("aggregate_type", row["aggregate_type"])
    record.setdefault("aggregate_id", str(row["aggregate_id"]))
    if "run_id" not in record:
        record["run_id"] = str(row["run_id"]) if row.get("run_id") else None
    record.setdefault("event_version", 1)

    return normalize_for_avro(record, schemas[topic], path=topic)


def serialize_event(
    topic: str,
    record: dict[str, Any],
    serializers: dict[str, AvroSerializer],
) -> bytes:
    serializer = serializers.get(topic)
    if serializer is None:
        raise ValueError(f"No Avro serializer configured for topic: {topic}")
    return serializer(record, SerializationContext(topic, MessageField.VALUE))
