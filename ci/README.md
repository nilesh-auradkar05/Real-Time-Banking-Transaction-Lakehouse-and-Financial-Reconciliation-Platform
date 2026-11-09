# Emulator build

`run_dbt_emulator.py` runs the dbt project against [fakesnow](https://github.com/tekumara/fakesnow), which executes Snowflake SQL on DuckDB in-process.

1. Create `RAW.CDC_EVENTS` from the production DDL in `snowflake/02_stage_and_raw.sql`.
2. Load the Debezium-shaped events from `fixtures.py`.
3. Run `dbt build` with target `ci`: every model and data test.
4. Check the outcomes in `fixtures.EXPECTED`.

## Fixture scenarios

| Scenario | Rule it proves |
|---|---|
| Same Kafka coordinates landed in two files | Transport dedup in `stg_cdc_events` |
| Faker transaction deleted | Deletes stay in facts as `is_deleted` |
| Czech and Faker rows | Every fact row keeps its real origin |
| Account frequency update between transactions | Facts pick the dimension version in force at commit |
| Loan status update between repayments | Same, for `dim_loan` |
| Matched ROB and CCW, unmatched ROB, plain deposit | `-1` / `-2` conditional keys |
| Stated balance 5.00 off on one day | `VARIANCE` in the daily balance check |
| Debezium re-snapshot re-sends a transaction, an account and a loan as `op = 'r'` | Facts keep the original commit time and SCD2 versions; no new dimension versions; `was_resnapshotted` set |

## Emulator differences

These keep CI runnable; none change the SQL sent to Snowflake.

| Difference | Handling |
|---|---|
| No `TRY_TO_TIMESTAMP_NTZ` | DuckDB macro in `EMULATOR_SHIMS` |
| DuckDB `HASH` is unsigned 64-bit and overflows `NUMBER(38,0)` after `ABS` | `hash_key` macro uses a modulo on the `ci` target only |
| `TO_CHAR(date, 'YYYYMMDD')` mistranslated | Models use the arithmetic `date_key` macro on every target |

Stages, `COPY INTO`, storage integrations and warehouse behaviour are not emulated.

## Known blind spots

DuckDB accepts some SQL that Snowflake rejects, so a green emulator run is necessary but not sufficient.

| Accepted here, rejected by Snowflake | Seen in |
|---|---|
| Correlated `NOT EXISTS` subquery with a non-equality predicate | `fact_account_balance_daily`, now an anti-join (`LEFT JOIN … WHERE successor.transaction_id IS NULL`) |
| `TRY_TO_NUMBER` without scale returning decimals; Snowflake returns `NUMBER(38,0)` | `07_copy_source_windows.sql`, now guarded by `tests/test_snowflake_copy.py` |
