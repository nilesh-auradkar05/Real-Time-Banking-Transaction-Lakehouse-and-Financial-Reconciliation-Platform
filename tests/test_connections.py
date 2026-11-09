import json

import pytest
from banking_pipeline.connections import missing, snowflake_connection_json

BASE = {"SNOWFLAKE_ACCOUNT": "org-acct", "SNOWFLAKE_USER": "loader", "SNOWFLAKE_PASSWORD": "p"}


def test_builds_provider_shaped_connection_with_defaults():
    conn = json.loads(snowflake_connection_json(BASE))
    assert conn["conn_type"] == "snowflake" and conn["login"] == "loader"
    assert conn["extra"] == {"account": "org-acct", "warehouse": "BANKING_ETL_WH",
                             "database": "BANKING_LAKEHOUSE", "role": "FINANCE_DBT_ROLE"}


def test_password_with_json_metacharacters_round_trips():
    tricky = 'a"b\\c{d}$e'
    conn = json.loads(snowflake_connection_json({**BASE, "SNOWFLAKE_PASSWORD": tricky}))
    assert conn["password"] == tricky


def test_overrides_are_respected():
    conn = json.loads(snowflake_connection_json({**BASE, "SNOWFLAKE_ROLE": "ACCOUNTADMIN"}))
    assert conn["extra"]["role"] == "ACCOUNTADMIN"


def test_missing_credentials_are_reported_not_defaulted():
    assert missing({"SNOWFLAKE_ACCOUNT": "x", "SNOWFLAKE_USER": " "}) == [
        "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    with pytest.raises(ValueError):
        snowflake_connection_json({})
