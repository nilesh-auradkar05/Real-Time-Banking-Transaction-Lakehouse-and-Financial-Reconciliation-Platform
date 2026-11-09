from pathlib import Path

import pytest
from banking_pipeline.snowflake_copy import copy_statement, read_copy_statement, summarize_copy

SNOWFLAKE_SQL = Path(__file__).resolve().parents[1] / "snowflake"


def test_no_new_files_is_a_success_with_zero_counts():
    summary = summarize_copy([("Copy executed with 0 files processed.",)])
    assert summary.ok and (summary.files_loaded, summary.rows_loaded) == (0, 0)


def test_loaded_and_partially_loaded_files_are_counted():
    rows = [("s3://b/a.parquet", "LOADED", 10, 10, 1, 0, None),
            ("s3://b/b.parquet", "PARTIALLY_LOADED", 5, 3, 5, 2, "bad row")]
    s = summarize_copy(rows)
    assert (s.files_loaded, s.rows_loaded, s.rows_rejected, s.ok) == (2, 13, 2, True)


def test_failed_file_marks_batch_failed_with_first_error():
    rows = [("s3://b/a.parquet", "LOAD_FAILED", 5, 0, 1, 5, "Numeric value 'x' is not recognized")]
    s = summarize_copy(rows)
    assert not s.ok and s.files_failed == 1 and s.first_error.startswith("s3://b/a.parquet")


def test_copy_statement_strips_use_prefix_and_trailing_semicolon():
    sql = "USE WAREHOUSE W;\nUSE DATABASE D;\n\nCOPY INTO RAW.T FROM @s;\n"
    assert copy_statement(sql) == "COPY INTO RAW.T FROM @s"


def test_missing_copy_statement_raises():
    with pytest.raises(ValueError):
        copy_statement("SELECT 1;")


@pytest.mark.parametrize("name", ["03_copy_into_raw.sql", "07_copy_source_windows.sql",
                                  "07b_copy_dead_letters.sql"])
def test_every_dag_copy_script_contains_one_statement(name):
    statement = read_copy_statement(SNOWFLAKE_SQL, name)
    assert statement.upper().startswith("COPY INTO") and ";" not in statement
