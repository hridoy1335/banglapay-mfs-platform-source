# BanglaPay Outbox Publisher Patch

Drop these files into the matching paths in the repository. The patch assumes the existing PostgreSQL table is `integration.outbox_events` and has the columns used in the project setup: `event_id`, `event_type`, `topic_name`, `aggregate_type`, `aggregate_id`, `event_payload`, `run_id`, `publish_status`, `attempt_count`, `available_at`, `locked_at`, `published_at`, `last_error`, and `created_at`.

## Install/update dependency

The existing `requirements.txt` should include:

```text
psycopg[binary,pool]
pymongo[srv]
confluent-kafka[avro]
python-dotenv
PyYAML
```

Then run `pip install -r requirements.txt` in the active virtual environment.

## 1. Sync schemas from Schema Registry

From the repository root:

```bash
python -m scripts.sync_avro_schemas
wc -c schemas/avro/*.avsc
```

This downloads the latest versions of the seven subjects already registered in Confluent. It does not register or modify schemas.

## 2. First pilot: one transaction event

```bash
python -m src.publishers.outbox_publisher --topic mfs.transaction.events.v1 --batch-size 1
```

Check the Outbox row and Confluent topic before increasing batch size.

## 3. Next batch

After the pilot is verified:

```bash
python -m src.publishers.outbox_publisher --batch-size 5
```

Each invocation publishes one batch and exits. It does not run continuously.

## Delivery behavior

- Claims rows with `FOR UPDATE SKIP LOCKED`, sets them to `PROCESSING`, increments `attempt_count`, and commits before Kafka network I/O.
- Uses the aggregate ID as the Kafka message key so events for the same aggregate are routed consistently to a partition.
- Marks an Outbox row `PUBLISHED` only after Kafka's delivery callback confirms success.
- Retries failures using exponential backoff; rows become `FAILED` after the configured maximum attempts.
- A stale `PROCESSING` claim can be reclaimed after the configured lease timeout.
- Kafka producer idempotence reduces producer-retry duplicates, but PostgreSQL and Kafka are not one distributed transaction. A crash after Kafka accepts a message but before PostgreSQL records `PUBLISHED` can cause a duplicate. Consumers must deduplicate using `event_id`.

## Important check before first run

The script expects the listed Outbox columns and `publish_status` values `PENDING`, `PROCESSING`, `PUBLISHED`, and `FAILED`. Verify against the live table if the schema has changed. Never commit `.env` or print API credentials.
