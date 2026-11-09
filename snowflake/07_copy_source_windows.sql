USE WAREHOUSE BANKING_ETL_WH;
USE DATABASE BANKING_LAKEHOUSE;

COPY INTO AUDIT.SOURCE_ACCOUNT_WINDOW (
    SNAPSHOT_RUN_ID, WINDOW_START, WINDOW_END, ACCOUNT_ID, DATA_ORIGIN,
    SOURCE_COUNT, SOURCE_AMOUNT, SOURCE_SNAPSHOT_AT, SOURCE_FILE
)
FROM (
    SELECT t.$1:snapshot_run_id::varchar,
        TRY_TO_DATE(t.$1:window_start::varchar),TRY_TO_DATE(t.$1:window_end::varchar),
        TRY_TO_NUMBER(t.$1:account_id::varchar),t.$1:data_origin::varchar,
        TRY_TO_NUMBER(t.$1:source_count::varchar),
        -- Scale is explicit: TO_NUMBER defaults to NUMBER(38,0) and would drop the cents.
        TRY_TO_NUMBER(t.$1:source_amount::varchar, 18, 2),
        TRY_TO_TIMESTAMP_TZ(t.$1:source_snapshot_at::varchar),METADATA$FILENAME
    FROM @RAW.BANKING_SOURCE_AUDIT_STAGE t)
FILE_FORMAT=(FORMAT_NAME=RAW.AUDIT_JSON_FMT)
PATTERN='.*[.]jsonl'
ON_ERROR='ABORT_STATEMENT';