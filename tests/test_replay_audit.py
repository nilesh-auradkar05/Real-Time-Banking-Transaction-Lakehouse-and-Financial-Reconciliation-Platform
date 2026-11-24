"""Replay audit sync against the fakesnow emulator, using the production DDL (09_*.sql)."""
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

fakesnow = pytest.importorskip("fakesnow")

from banking_pipeline.replay_audit import watermark, write  # noqa: E402

DDL = Path(__file__).resolve().parents[1] / "snowflake" / "09_replay_audit_tables.sql"
T0 = datetime(2026, 11, 1, tzinfo=timezone.utc)


def at(minutes):
    return T0 + timedelta(minutes=minutes)


def run(rid, status):
    return (rid, "default", 42, Decimal("0.00"), at(0), None if status == "running" else at(30), status)


def event(eid, rid, pk, committed, fault=None, delay=0):
    return (eid, rid, "fin_trans", pk, "c", "czech_original", date(2013, 1, 5), Decimal("10.00"),
            at(1), committed, delay, fault)


@pytest.fixture()
def cur():
    with fakesnow.patch():
        import snowflake.connector

        conn = snowflake.connector.connect(database="BANKING_LAKEHOUSE")
        c = conn.cursor()
        c.execute("CREATE DATABASE IF NOT EXISTS BANKING_LAKEHOUSE")
        c.execute("USE DATABASE BANKING_LAKEHOUSE")
        c.execute("CREATE SCHEMA IF NOT EXISTS BANKING_LAKEHOUSE.AUDIT")
        for stmt in re.split(r";\s*\n", DDL.read_text()):
            body = "\n".join(ln for ln in stmt.splitlines() if not ln.strip().startswith("--")).strip()
            if body.upper().startswith(("CREATE", "DROP")):
                c.execute(body)
        yield c


def rows(cur, sql):
    return [tuple(r) for r in cur.execute(sql).fetchall()]


def test_first_sync_loads_runs_and_events_from_watermark_zero(cur):
    assert watermark(cur) == 0
    res = write(cur, [run(1, "completed")], [[event(1, 1, 101, at(2)), event(2, 1, 102, at(3))]],
                synced_at=at(10), wm=0)
    assert (res.runs, res.events) == (1, 2)
    assert rows(cur, "SELECT COUNT(*) FROM AUDIT.REPLAY_EVENT") == [(2,)]
    assert watermark(cur) == 3  # everything committed: continue after the newest id


def test_resync_is_idempotent(cur):
    for _ in range(2):
        write(cur, [run(1, "completed")], [[event(1, 1, 101, at(2))]], synced_at=at(10), wm=0)
    assert rows(cur, "SELECT COUNT(*) FROM AUDIT.REPLAY_RUN") == [(1,)]
    assert rows(cur, "SELECT COUNT(*) FROM AUDIT.REPLAY_EVENT") == [(1,)]


def test_late_commit_is_picked_up_on_the_next_sync(cur):
    # Planned late arrival: the row exists before its change commits.
    write(cur, [run(1, "running")], [[event(1, 1, 101, at(2)), event(2, 1, 102, None, "late_arrival", 5)]],
          synced_at=at(10), wm=0)
    assert watermark(cur) == 2  # held at the uncommitted event of a running replay
    write(cur, [run(1, "completed")], [[event(2, 1, 102, at(12), "late_arrival", 5)]],
          synced_at=at(20), wm=2)
    got = rows(cur, "SELECT ACTUAL_COMMIT_AT IS NOT NULL, INJECTED_FAULT FROM AUDIT.REPLAY_EVENT "
                    "WHERE REPLAY_EVENT_ID = 2")
    assert got == [(True, "late_arrival")]
    assert watermark(cur) == 3


def test_uncommitted_events_of_finished_runs_do_not_hold_the_watermark(cur):
    write(cur, [run(1, "interrupted")], [[event(1, 1, 101, None), event(2, 1, 102, at(3))]],
          synced_at=at(10), wm=0)
    assert watermark(cur) == 3  # event 1 is final NOT_COMMITTED, not pending


def test_ddl_file_is_rerunnable(cur):
    """09 is applied again whenever it gains a table, so every statement must be re-runnable."""
    for _ in range(2):
        for stmt in re.split(r";\s*\n", DDL.read_text()):
            body = "\n".join(ln for ln in stmt.splitlines() if not ln.strip().startswith("--")).strip()
            if body and not body.upper().startswith("USE"):
                cur.execute(body)
    assert rows(cur, "SELECT COUNT(*) FROM AUDIT.RECON_ACKNOWLEDGEMENT") == [(0,)]
