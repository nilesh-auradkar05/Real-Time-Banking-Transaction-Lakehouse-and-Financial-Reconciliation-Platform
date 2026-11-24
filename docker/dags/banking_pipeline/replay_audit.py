"""Copy replay_run and replay_event from PostgreSQL into Snowflake AUDIT.

These rows are the *expected* side of replay-delivery reconciliation: every change the simulator
planned and committed. They are copied directly, outside Debezium and Kafka, so a fault in the
pipeline under test cannot also lose the evidence of what should have arrived.

Kept free of Airflow imports; connections are injected so CI runs the same code on fakesnow.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Iterator, Sequence

RUN_COLUMNS = ("replay_run_id", "scenario", "random_seed", "speed_factor",
               "started_at", "ended_at", "status")
EVENT_COLUMNS = ("replay_event_id", "replay_run_id", "source_table", "record_pk", "operation",
                 "data_origin", "business_date", "amount", "intended_commit_at",
                 "actual_commit_at", "injected_delay_seconds", "injected_fault")

# Column types of the session-scoped staging tables (match 09_replay_audit_tables.sql).
_TYPES = {
    "replay_run_id": "NUMBER(38,0)", "scenario": "VARCHAR", "random_seed": "NUMBER(38,0)",
    "speed_factor": "NUMBER(10,2)", "started_at": "TIMESTAMP_TZ", "ended_at": "TIMESTAMP_TZ",
    "status": "VARCHAR", "replay_event_id": "NUMBER(38,0)", "source_table": "VARCHAR",
    "record_pk": "NUMBER(38,0)", "operation": "VARCHAR", "data_origin": "VARCHAR",
    "business_date": "DATE", "amount": "NUMBER(12,2)", "intended_commit_at": "TIMESTAMP_TZ",
    "actual_commit_at": "TIMESTAMP_TZ", "injected_delay_seconds": "NUMBER(38,0)",
    "injected_fault": "VARCHAR", "synced_at": "TIMESTAMP_TZ",
}

# Re-read from the oldest event that is still uncommitted inside a running replay: its
# actual_commit_at is stamped later (late arrivals are planned first, committed after a delay).
# Otherwise continue after the newest synced id. Finished runs' NULLs are final (NOT_COMMITTED),
# so they never hold the watermark back.
WATERMARK_SQL = """
SELECT COALESCE(
    (SELECT MIN(e.REPLAY_EVENT_ID)
       FROM AUDIT.REPLAY_EVENT e
       JOIN AUDIT.REPLAY_RUN r ON r.REPLAY_RUN_ID = e.REPLAY_RUN_ID
      WHERE e.ACTUAL_COMMIT_AT IS NULL AND r.STATUS = 'running'),
    (SELECT MAX(REPLAY_EVENT_ID) + 1 FROM AUDIT.REPLAY_EVENT),
    0)
"""

RUNS_SOURCE_SQL = f"SELECT {', '.join(RUN_COLUMNS)} FROM replay_run ORDER BY replay_run_id"
EVENTS_SOURCE_SQL = (f"SELECT {', '.join(EVENT_COLUMNS)} FROM replay_event "
                     "WHERE replay_event_id >= %s ORDER BY replay_event_id")

# Snowflake rejects statements over 1 MB; ~200 bytes per rendered row keeps a batch near 400 KB.
DEFAULT_BATCH_SIZE = 2000


@dataclass(frozen=True)
class SyncResult:
    watermark: int
    runs: int
    events: int


def read_runs(pg_conn) -> list[tuple]:
    with pg_conn.cursor() as cur:
        cur.execute(RUNS_SOURCE_SQL)
        return list(cur.fetchall())


def read_events(pg_conn, min_event_id: int, batch_size: int = DEFAULT_BATCH_SIZE) -> Iterator[list[tuple]]:
    """Stream replay_event rows in id order with a server-side cursor."""
    with pg_conn.cursor(name="replay_event_sync") as cur:
        cur.itersize = batch_size
        cur.execute(EVENTS_SOURCE_SQL, (min_event_id,))
        while rows := cur.fetchmany(batch_size):
            yield list(rows)


def watermark(sf_cursor) -> int:
    sf_cursor.execute(WATERMARK_SQL)
    return int(sf_cursor.fetchone()[0])


def _merge(sf_cursor, table: str, columns: Sequence[str], key: str,
           batches: Iterable[Sequence[tuple]], synced_at: datetime,
           batch_size: int = DEFAULT_BATCH_SIZE) -> int:
    # Session-scoped and unqualified: it lands in the session's current schema and disappears
    # with the session, so concurrent runs cannot collide.
    staging = f"{table}_SYNC"
    cols = (*columns, "synced_at")
    ddl = ", ".join(f"{c.upper()} {_TYPES[c]}" for c in cols)
    sf_cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE {staging} ({ddl})")
    insert = (f"INSERT INTO {staging} ({', '.join(c.upper() for c in cols)}) "
              f"VALUES ({', '.join(['%s'] * len(cols))})")
    total = 0
    for batch in batches:
        for start in range(0, len(batch), batch_size):
            chunk = [(*row, synced_at) for row in batch[start:start + batch_size]]
            sf_cursor.executemany(insert, chunk)
            total += len(chunk)
    if total:
        updates = ", ".join(f"t.{c.upper()} = s.{c.upper()}" for c in cols if c != key)
        names = ", ".join(c.upper() for c in cols)
        values = ", ".join(f"s.{c.upper()}" for c in cols)
        sf_cursor.execute(
            f"MERGE INTO AUDIT.{table} t USING {staging} s ON t.{key.upper()} = s.{key.upper()} "
            f"WHEN MATCHED THEN UPDATE SET {updates} "
            f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})")
    sf_cursor.execute(f"DROP TABLE IF EXISTS {staging}")
    return total


def write(sf_cursor, runs: Sequence[tuple], event_batches: Iterable[Sequence[tuple]],
          synced_at: datetime, wm: int) -> SyncResult:
    """MERGE runs, then events. synced_at must be the time the sync STARTED reading the source:
    every change committed before it already had its replay_event row written."""
    n_runs = _merge(sf_cursor, "REPLAY_RUN", RUN_COLUMNS, "replay_run_id", [runs], synced_at)
    n_events = _merge(sf_cursor, "REPLAY_EVENT", EVENT_COLUMNS, "replay_event_id",
                      event_batches, synced_at)
    return SyncResult(watermark=wm, runs=n_runs, events=n_events)


def sync(pg_conn, sf_cursor, synced_at: datetime, batch_size: int = DEFAULT_BATCH_SIZE) -> SyncResult:
    wm = watermark(sf_cursor)
    return write(sf_cursor, read_runs(pg_conn), read_events(pg_conn, wm, batch_size), synced_at, wm)