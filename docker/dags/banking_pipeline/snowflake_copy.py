"""Parse Snowflake COPY INTO statements and results.

Kept free of Airflow and Snowflake imports so it is unit-tested in CI.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

# COPY INTO <table> result columns: file, status, rows_parsed, rows_loaded, error_limit,
# errors_seen, first_error, first_error_line, ... When no new files exist, Snowflake returns a
# single one-column row: "Copy executed with 0 files processed."
_FILE, _STATUS, _ROWS_PARSED, _ROWS_LOADED, _ERROR_LIMIT, _ERRORS_SEEN, _FIRST_ERROR = range(7)
LOADED_STATUSES = frozenset({"LOADED", "PARTIALLY_LOADED"})
FAILED_STATUSES = frozenset({"LOAD_FAILED"})


@dataclass(frozen=True)
class CopySummary:
    files_loaded: int = 0
    rows_loaded: int = 0
    rows_rejected: int = 0
    files_failed: int = 0
    first_error: str | None = None

    @property
    def ok(self) -> bool:
        return self.files_failed == 0


def copy_statement(sql_text: str) -> str:
    """Return the COPY INTO statement from a script that may start with USE statements."""
    upper = sql_text.upper()
    start = upper.find("COPY INTO")
    if start < 0:
        raise ValueError("No COPY INTO statement found")
    statement = sql_text[start:].strip()
    return statement[:-1].rstrip() if statement.endswith(";") else statement


def read_copy_statement(sql_home: Path, file_name: str) -> str:
    return copy_statement((sql_home / file_name).read_text())


def summarize_copy(rows: Iterable[Sequence]) -> CopySummary:
    files = loaded = rejected = failed = 0
    first_error = None
    for row in rows:
        if len(row) <= _ERRORS_SEEN:  # "Copy executed with 0 files processed."
            continue
        status = str(row[_STATUS]).upper()
        if status in LOADED_STATUSES:
            files += 1
            loaded += int(row[_ROWS_LOADED] or 0)
            rejected += int(row[_ERRORS_SEEN] or 0)
        elif status in FAILED_STATUSES:
            failed += 1
            rejected += int(row[_ERRORS_SEEN] or 0)
            if first_error is None and len(row) > _FIRST_ERROR:
                first_error = f"{row[_FILE]}: {row[_FIRST_ERROR]}"
    return CopySummary(files, loaded, rejected, failed, first_error)
