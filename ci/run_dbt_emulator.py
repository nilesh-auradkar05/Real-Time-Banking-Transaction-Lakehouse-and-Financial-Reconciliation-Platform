"""Build the dbt project against fakesnow (DuckDB-backed Snowflake emulator) and assert outcomes.

Usage: python ci/run_dbt_emulator.py
Proves the Bronze -> Silver -> Gold models compile, run and pass their tests on CDC fixtures
without Snowflake credentials. Emulator gaps (not Snowflake behaviour) are listed in ci/README.md.
"""
from __future__ import annotations

import os
import re
import sys
import tempfile
from pathlib import Path

import fakesnow

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
import fixtures  # noqa: E402

COLUMNS = ["event_id", "topic", "kafka_partition", "kafka_offset", "kafka_timestamp_ms",
           "source_database", "source_schema", "source_table", "op", "record_pk",
           "replay_event_id", "data_origin", "synthetic_batch_id", "event_ts_ms",
           "debezium_ts_ms", "source_lsn", "source_txid", "snapshot", "key_json",
           "before_json", "after_json", "landing_ingested_at", "src_filename",
           "src_file_row_number", "raw_loaded_at"]
VARIANTS = {"key_json", "before_json", "after_json"}


def raw_table_ddl() -> str:
    """Use the production DDL so CI breaks if RAW.CDC_EVENTS drifts from the models."""
    text = (ROOT / "snowflake" / "02_stage_and_raw.sql").read_text()
    match = re.search(r"CREATE TABLE IF NOT EXISTS RAW\.CDC_EVENTS\s*\(.*?\);", text, re.S)
    if not match:
        raise SystemExit("RAW.CDC_EVENTS DDL not found in snowflake/02_stage_and_raw.sql")
    return match.group(0)


# Snowflake functions fakesnow does not translate yet. Each shim mirrors Snowflake semantics for
# the inputs these models use; it runs only in CI and never changes the production SQL.
EMULATOR_SHIMS = {
    "try_to_timestamp_ntz(x)": "TRY_CAST(x AS TIMESTAMP)",
}
SCHEMAS = ("RAW", "SILVER", "GOLD", "AUDIT")


def install_shims(conn) -> None:
    """fakesnow gives dbt its own DuckDB cursor, so TEMP macros are invisible to it.
    Create the shims in every schema dbt resolves unqualified functions against."""
    for schema in SCHEMAS:
        for signature, body in EMULATOR_SHIMS.items():
            conn._duck_conn.execute(
                f"CREATE OR REPLACE MACRO BANKING_LAKEHOUSE.{schema}.{signature} AS {body}")


def seed(conn) -> None:
    cur = conn.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS BANKING_LAKEHOUSE")
    cur.execute("USE DATABASE BANKING_LAKEHOUSE")
    for schema in SCHEMAS:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS BANKING_LAKEHOUSE.{schema}")
    cur.execute(raw_table_ddl())
    select = ", ".join(f"PARSE_JSON(%({c})s)" if c in VARIANTS else f"%({c})s" for c in COLUMNS)
    sql = f"INSERT INTO RAW.CDC_EVENTS ({', '.join(COLUMNS)}) SELECT {select}"
    rows = fixtures.build()
    for row in rows:
        cur.execute(sql, {c: row[c] for c in COLUMNS})
    print(f"seeded RAW.CDC_EVENTS with {len(rows)} landed rows")


def run_dbt(work: Path) -> bool:
    from dbt.cli.main import dbtRunner

    os.environ["DBT_TARGET"] = "ci"
    project = ROOT / "banking_dbt"
    result = dbtRunner().invoke([
        "build", "--project-dir", str(project), "--profiles-dir", str(project / "profiles"),
        "--target-path", str(work / "target"), "--log-path", str(work / "logs"),
        "--threads", "1",
    ])
    return bool(result.success)


def check(conn) -> list[str]:
    failures = []
    cur = conn.cursor()
    for name, (sql, expected) in fixtures.EXPECTED.items():
        actual = [tuple(r) for r in cur.execute(sql).fetchall()]
        if actual != expected:
            failures.append(f"{name}\n    expected {expected}\n    actual   {actual}")
            print(f"FAIL  {name}")
        else:
            print(f"ok    {name}")
    return failures


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp, fakesnow.patch(
        "snowflake.connector.connect", "dbt.adapters.snowflake.connections.snowflake.connector.connect"
    ):
        import snowflake.connector

        conn = snowflake.connector.connect(database="BANKING_LAKEHOUSE")
        seed(conn)
        install_shims(conn)
        if not run_dbt(Path(tmp)):
            print("dbt build failed", file=sys.stderr)
            return 1
        failures = check(conn)
    for f in failures:
        print(f, file=sys.stderr)
    print(f"{len(fixtures.EXPECTED) - len(failures)}/{len(fixtures.EXPECTED)} outcome checks passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
