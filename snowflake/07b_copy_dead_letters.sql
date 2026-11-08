USE WAREHOUSE BANKING_ETL_WH;
USE DATABASE BANKING_LAKEHOUSE;

COPY INTO AUDIT.DEAD_LETTER_EVENT (
    EVENT_ID,TOPIC,KAFKA_PARTITION,KAFKA_OFFSET,ERROR_CLASS,ERROR_MESSAGE,
    RETRY_COUNT,FIRST_FAILED_AT,RESOLVED_AT,SRC_FILENAME
)
FROM (
    SELECT t.$1:event_id::varchar,t.$1:topic::varchar,
    TRY_TO_NUMBER(t.$1:kafka_parition::varchar),
    TRY_TO_NUMBER(t.$1:kafka_offset::varchar),
    t.$1:error_class::varchar,t.$1:error_message::varchar,
    TRY_TO_NUMBER(t.$1:retry_count::varchar),
    TRY_TO_TIMESTAMP_TZ(t.$1:first_failed_at::varchar),
    TRY_TO_TIMESTAMP_TZ(t.$1:resolved_at::varchar),METADATA$FILENAME
    FROM @RAW.BANKING_DLQ_STAGE t
)
FILE_FORMAT=(FORMAT_NAME=RAW.AUDIT_JSON_FMT)
PATTERN='.*[.]json'
ON_ERROR='ABORT_STATEMENT';