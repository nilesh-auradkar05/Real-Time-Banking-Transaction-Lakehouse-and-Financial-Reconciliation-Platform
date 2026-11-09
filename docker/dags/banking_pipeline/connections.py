"""Build the Airflow Snowflake connection from the same SNOWFLAKE_* variables dbt and
scripts/reconcile.py read (snowflake/.env), so credentials live in one place.

The image entrypoint runs this module and exports the result as AIRFLOW_CONN_SNOWFLAKE_BANKING.
JSON is built with json.dumps, so passwords containing quotes or backslashes stay valid;
interpolating them into a JSON string in docker-compose would not.
"""
from __future__ import annotations

import json
import os
import sys
from typing import Mapping

REQUIRED = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")
DEFAULTS = {"SNOWFLAKE_ROLE": "FINANCE_DBT_ROLE", "SNOWFLAKE_WAREHOUSE": "BANKING_ETL_WH",
            "SNOWFLAKE_DATABASE": "BANKING_LAKEHOUSE"}


def missing(env: Mapping[str, str]) -> list[str]:
    return [name for name in REQUIRED if not env.get(name, "").strip()]


def snowflake_connection_json(env: Mapping[str, str]) -> str:
    absent = missing(env)
    if absent:
        raise ValueError(f"missing {', '.join(absent)}")

    def get(name: str) -> str:
        return env.get(name, "").strip() or DEFAULTS[name]

    return json.dumps({
        "conn_type": "snowflake",
        "login": env["SNOWFLAKE_USER"].strip(),
        "password": env["SNOWFLAKE_PASSWORD"],
        "schema": "RAW",
        "extra": {
            "account": env["SNOWFLAKE_ACCOUNT"].strip(),
            "warehouse": get("SNOWFLAKE_WAREHOUSE"),
            "database": get("SNOWFLAKE_DATABASE"),
            "role": get("SNOWFLAKE_ROLE"),
        },
    })


if __name__ == "__main__":
    try:
        print(snowflake_connection_json(os.environ))
    except ValueError as exc:
        print(f"snowflake connection not configured: {exc}", file=sys.stderr)
        sys.exit(2)
