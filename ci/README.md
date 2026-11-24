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
| Replay ids on every replayed change, synced through `replay_audit.write` | `DELIVERED` for inserts, updates and the delete (matched on key) |
| Transaction 3 re-emitted at a new Kafka offset | `DUPLICATE_DELIVERY` |
| Client 1 update committed, never delivered | `MISSING`; `assert_no_missing_replay_events` fires on exactly that row |
| Order update committed 5 minutes before `as_of`; account update planned in a running replay | `PENDING` |
| Event from an interrupted replay with no commit | `NOT_COMMITTED` |
| Client 2 update with a replay id the expected set lacks | `UNEXPECTED_CHANGE`; `assert_no_unexpected_replay_changes` fires on exactly that row |
| Injected `late_arrival` (intended minute 45, committed 50) | Delivered, `commit_delay_seconds = 300` |
| Order 701 update lost; re-snapshot carries its state and replay id | `RECOVERED_BY_SNAPSHOT`; the warn test fires |
| Client 1 re-snapshotted *without* its lost update | Stays `MISSING`: a snapshot of the right row is not enough, it must carry the change |
| Account 10 update lost, acknowledged after commit | `MISSING`, `is_acknowledged`; excluded from the gate |
| Run-wide acknowledgement given before the losses committed | Covers nothing |

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