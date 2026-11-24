#!/usr/bin/env python3
"""Fail when Debezium can no longer capture changes from PostgreSQL.

Catches the silent failure from 2026-11-23: PostgreSQL was re-initialised, its WAL restarted
near zero, but Kafka Connect still held the old saved LSN. Debezium then skipped every new
change as "already processed", with the slot active and no error in any log.

Checks:
  1. the replication slot exists and is active
  2. Debezium's saved offset LSN is not ahead of pg_current_wal_lsn()

    uv run python scripts/check_cdc_position.py

Environment (defaults suit the host; inside Compose use KAFKA_BOOTSTRAP_SERVERS=kafka:9092,
POSTGRES_HOST=postgres, POSTGRES_PORT=5432):
  KAFKA_BOOTSTRAP_SERVERS, OFFSETS_TOPIC, CONNECTOR_NAME, SLOT_NAME,
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB
Exit code 0 = healthy, 1 = problem found, 2 = could not check.
"""
import json
import os
import sys
import time
from typing import Iterable

import psycopg2
from confluent_kafka import Consumer, KafkaException, TopicPartition
from dotenv import load_dotenv

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
OFFSETS_TOPIC = os.environ.get("OFFSETS_TOPIC", "connect-offsets")
CONNECTOR = os.environ.get("CONNECTOR_NAME", "postgres-connector")
SLOT = os.environ.get("SLOT_NAME", "banking_slot")


def latest_lsn(records: Iterable[tuple[bytes | None, bytes | None]], connector: str) -> int | None:
    """Highest current LSN across the connector's source partitions, from offsets-topic records
    in log order. The latest record per key wins; a null value (offset reset) clears it."""
    latest: dict[str, int | None] = {}
    for key, value in records:
        if not key:
            continue
        name, source_partition = json.loads(key)
        if name != connector:
            continue
        offset = json.loads(value) if value else None
        lsn = offset.get("lsn") if offset else None
        latest[json.dumps(source_partition, sort_keys=True)] = None if lsn is None else int(lsn)
    lsns = [lsn for lsn in latest.values() if lsn is not None]
    return max(lsns) if lsns else None  # None: no offset yet (fresh connector, snapshot pending)


def saved_offset_lsn(timeout_s: float = 30) -> int | None:
    """Debezium's saved LSN, read from Kafka Connect's offsets topic. debezium/connect:2.2 runs
    Kafka Connect 3.4, which has no offsets REST API (added in 3.5)."""
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "cdc-position-check",
                         "enable.auto.commit": False})
    try:
        topic = consumer.list_topics(OFFSETS_TOPIC, timeout=10).topics[OFFSETS_TOPIC]
        if topic.error:
            raise KafkaException(topic.error)
        ends, start = {}, []
        for p in topic.partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(OFFSETS_TOPIC, p), timeout=10)
            if high > low:
                ends[p] = high
                start.append(TopicPartition(OFFSETS_TOPIC, p, low))
        consumer.assign(start)
        records, deadline = [], time.monotonic() + timeout_s
        while ends:
            if time.monotonic() > deadline:
                raise TimeoutError(f"{OFFSETS_TOPIC} not read to its end in {timeout_s}s")
            msg = consumer.poll(1.0)
            if msg is None:
                # Compaction can leave the high watermark past the last record.
                for tp in consumer.position([TopicPartition(OFFSETS_TOPIC, p) for p in ends]):
                    if tp.offset >= ends[tp.partition]:
                        ends.pop(tp.partition)
                continue
            if msg.error():
                raise KafkaException(msg.error())
            records.append((msg.key(), msg.value()))
            if msg.offset() + 1 >= ends.get(msg.partition(), 0):
                ends.pop(msg.partition(), None)
        return latest_lsn(records, CONNECTOR)
    finally:
        consumer.close()


def postgres_positions() -> tuple[int, tuple[bool, int] | None]:
    """(current WAL LSN, (slot active, slot confirmed_flush LSN) or None), as integers."""
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5434")),
        user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"], application_name="cdc-position-check")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint")
            wal = cur.fetchone()[0]
            cur.execute("SELECT active, pg_wal_lsn_diff(confirmed_flush_lsn, '0/0')::bigint "
                        "FROM pg_replication_slots WHERE slot_name = %s", (SLOT,))
            slot = cur.fetchone()
        return wal, slot
    finally:
        conn.close()


def fmt(lsn: int | None) -> str:
    return "none" if lsn is None else f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


def find_problems(saved: int | None, wal: int, slot: tuple[bool, int] | None) -> list[str]:
    problems = []
    if slot is None:
        problems.append(f"replication slot {SLOT} does not exist")
    elif not slot[0]:
        problems.append(f"replication slot {SLOT} is not active (connector stopped or failed)")
    if saved is not None and saved > wal:
        problems.append(f"Debezium's saved LSN {fmt(saved)} is ahead of the current WAL {fmt(wal)}: "
                        "PostgreSQL was re-initialised and new changes are being skipped. "
                        "Reset the connector's offsets before trusting any new data.")
    return problems


def main() -> int:
    load_dotenv()
    try:
        saved = saved_offset_lsn()
        wal, slot = postgres_positions()
    except (KafkaException, TimeoutError, KeyError, ValueError, psycopg2.Error) as exc:
        print(f"ERROR: could not check CDC position: {exc}", file=sys.stderr)
        return 2

    problems = find_problems(saved, wal, slot)
    print(f"wal={fmt(wal)} saved_offset={fmt(saved)} "
          f"slot_confirmed={fmt(slot[1]) if slot else 'none'} slot_active={slot[0] if slot else None}")
    if problems:
        for p in problems:
            print(f"FAIL: {p}", file=sys.stderr)
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
