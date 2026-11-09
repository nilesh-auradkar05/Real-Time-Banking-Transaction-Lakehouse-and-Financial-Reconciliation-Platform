# banking_dbt

dbt project that turns landed Debezium envelopes in `RAW.CDC_EVENTS` into the Czech banking star schema. Modelling rules live in [`../docs/data_model.md`](../docs/data_model.md).

| Layer | Models | Materialization |
|---|---|---|
| Staging | `stg_cdc_events`: one row per Kafka topic, partition and offset | view |
| Silver | `fin_*` current state per source key, deletes kept as `is_deleted`; `fin_{account,client,loan}_history` SCD2 versions | table |
| Gold | Shared dimensions, `fact_transaction`, `fact_account_balance_daily`, `fact_loan`, `bridge_account_client`, marts | table |

## Conventions

- **Event order** is WAL position, then commit time, then Kafka coordinates (`cdc_order_desc`).
- **SCD2** comes from one macro, `cdc_scd2_history`. Versions run on the source commit clock and each key's first version starts at `1900-01-01`.
- **Origin** is the source value, `czech_original` or `faker_synthetic`. Anything else fails `assert_origin_known`.
- **Conditional keys** `order_key`, `loan_key` and `card_key` are `-1` when not applicable and `-2` when expected but unmatched.
- **Daily balances** chain on the natural `account_id`, because `account_key` changes with every SCD2 version.

## Run

```bash
# Snowflake: credentials from the environment (see profiles/profiles.yml)
dbt build --project-dir banking_dbt --profiles-dir banking_dbt/profiles

# No credentials: build and check on the fakesnow emulator, as CI does
uv run python ci/run_dbt_emulator.py
```
