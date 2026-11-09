USE WAREHOUSE BANKING_ETL_WH;
USE DATABASE BANKING_LAKEHOUSE;

CREATE SCHEMA IF NOT EXISTS AUDIT;

CREATE TABLE IF NOT EXISTS AUDIT.LOAD_BATCH (
    load_batch_id NUMBER(38,0) AUTOINCREMENT PRIMARY KEY,
    airflow_dag_id VARCHAR,airflow_run_id VARCHAR,target_table VARCHAR,
    window_start TIMESTAMP_TZ,window_end TIMESTAMP_TZ,
    files_loaded NUMBER(38,0),rows_loaded NUMBER(38,0),rows_rejected NUMBER(38,0),
    status VARCHAR,error_message VARCHAR,started_at TIMESTAMP_TZ,finished_at TIMESTAMP_TZ
);

CREATE TABLE IF NOT EXISTS AUDIT.DEAD_LETTER_EVENT (
    event_id VARCHAR, topic VARCHAR,kafka_partition NUMBER,kafka_offset NUMBER,
    error_class VARCHAR,error_message VARCHAR,retry_count NUMBER,
    first_failed_at TIMESTAMP_TZ,resolved_at TIMESTAMP_TZ,
    src_filename VARCHAR
);

CREATE TABLE IF NOT EXISTS AUDIT.RECON_RUN (
    recon_run_id NUMBER(38,0) AUTOINCREMENT PRIMARY KEY,
    snapshot_run_id VARCHAR,window_start DATE,window_end DATE,
    amount_tolerance NUMBER(18,2),accounts_checked NUMBER,accounts_mismatched NUMBER,
    status VARCHAR,started_at TIMESTAMP_TZ,finished_at TIMESTAMP_TZ
);

CREATE TABLE IF NOT EXISTS AUDIT.RECON_ACCOUNT_WINDOW (
    recon_run_id NUMBER(38,0),account_id NUMBER(38,0),data_origin VARCHAR,
    source_count NUMBER,target_count NUMBER,count_diff NUMBER,
    source_amount NUMBER(18,2),target_amount NUMBER(18,2),amount_diff NUMBER(18,2),status VARCHAR
);

CREATE TABLE IF NOT EXISTS AUDIT.RECON_EXCEPTION (
    recon_exception_id NUMBER(38,0) AUTOINCREMENT PRIMARY KEY,
    recon_run_id NUMBER(38,0),account_id NUMBER(38,0),exception_type VARCHAR,
    cdc_event_id VARCHAR,load_batch_id NUMBER(38,0),detail VARCHAR,
    detected_at TIMESTAMP_TZ,resolved_at TIMESTAMP_TZ
);

CREATE TABLE IF NOT EXISTS AUDIT.SOURCE_ACCOUNT_WINDOW (
    snapshot_run_id VARCHAR,window_start DATE,window_end DATE,account_id NUMBER(38,0),
    data_origin VARCHAR,source_count NUMBER,source_amount NUMBER(18,2),
    source_snapshot_at TIMESTAMP_TZ,source_file VARCHAR
);

-- Logical CDC transport entity. Transport identity is Kafka coordinates, not a generated sequence.

-- Logical CDC transport entity.
-- Transport identity is Kafka coordinates, not a generated sequence.
CREATE OR REPLACE VIEW AUDIT.CDC_EVENT AS
SELECT
    event_id AS cdc_event_id,
    topic,
    kafka_partition,
    kafka_offset,

    COALESCE(
        replay_event_id,
        TRY_TO_NUMBER(after_json:last_replay_event_id::VARCHAR),
        TRY_TO_NUMBER(before_json:last_replay_event_id::VARCHAR)
    ) AS replay_event_id,

    source_table,

    COALESCE(
        record_pk,
        TRY_TO_NUMBER(key_json:trans_id::VARCHAR),
        TRY_TO_NUMBER(key_json:account_id::VARCHAR),
        TRY_TO_NUMBER(key_json:client_id::VARCHAR)
    ) AS record_pk,

    op AS operation,
    source_lsn,

    TO_TIMESTAMP_TZ(event_ts_ms, 3) AS source_committed_at,
    TO_TIMESTAMP_TZ(kafka_timestamp_ms, 3) AS kafka_at,

    raw_loaded_at AS landed_at,
    src_filename AS landing_file,

    before_json AS before_payload,
    after_json AS after_payload

FROM RAW.CDC_EVENTS;

SELECT
    cdc_event_id,
    topic,
    kafka_partition,
    kafka_offset,
    operation,
    source_committed_at,
    kafka_at,
    landed_at
FROM AUDIT.CDC_EVENT
LIMIT 10;


-- Optional CDC replay source controls. Views remain empty until these tables are captured.
CREATE OR REPLACE VIEW AUDIT.REPLAY_EVENT AS
SELECT try_to_number(after_json:replay_event_id::varchar) AS replay_event_id,
       try_to_number(after_json:replay_run_id::varchar) AS replay_run_id,
       after_json:source_table::varchar AS source_table,
       try_to_number(after_json:record_pk::varchar) AS record_pk,
       after_json:operation::varchar AS operation,
       after_json:data_origin::varchar AS data_origin,
       after_json:business_date::varchar AS business_date,
       TRY_TO_NUMBER(after_json:amount::varchar, 18, 2) AS amount,
       event_id AS cdc_transport_id
FROM RAW.CDC_EVENTS WHERE source_table='replay_event' AND op IN ('c','r','u');

CREATE OR REPLACE VIEW AUDIT.REPLAY_RUN AS
SELECT try_to_number(after_json:replay_run_id::varchar) AS replay_run_id,
       after_json:scenario::varchar AS scenario,
       after_json:status::varchar AS status,
       event_id AS cdc_transport_id
FROM RAW.CDC_EVENTS WHERE source_table='replay_run' AND op IN ('c','r','u');