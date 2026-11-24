-- Expected changes for replay-delivery reconciliation.
-- Airflow (banking_s3_snowflake.sync_replay_audit) copies replay_run and replay_event from
-- PostgreSQL into these tables with MERGE, outside Debezium and Kafka, so the expected set
-- does not travel through the pipeline being tested. Run once, after 05_audit_control_tables.sql.
USE WAREHOUSE BANKING_ETL_WH;
USE DATABASE BANKING_LAKEHOUSE;

-- Safe to re-run: every statement is CREATE ... IF NOT EXISTS.
-- Upgrading a deployment whose older 05_audit_control_tables.sql created AUDIT.REPLAY_EVENT and
-- AUDIT.REPLAY_RUN as views? Drop those two views once, by hand, before running this file.
-- (Not done here: DROP VIEW IF EXISTS fails once the names are tables, which broke re-runs.)

CREATE TABLE IF NOT EXISTS AUDIT.REPLAY_RUN (
    REPLAY_RUN_ID   NUMBER(38,0) NOT NULL PRIMARY KEY,
    SCENARIO        VARCHAR,
    RANDOM_SEED     NUMBER(38,0),
    SPEED_FACTOR    NUMBER(10,2),
    STARTED_AT      TIMESTAMP_TZ,
    ENDED_AT        TIMESTAMP_TZ,
    STATUS          VARCHAR,          -- running, completed, interrupted, failed
    SYNCED_AT       TIMESTAMP_TZ NOT NULL
);

CREATE TABLE IF NOT EXISTS AUDIT.REPLAY_EVENT (
    REPLAY_EVENT_ID         NUMBER(38,0) NOT NULL PRIMARY KEY,
    REPLAY_RUN_ID           NUMBER(38,0) NOT NULL,
    SOURCE_TABLE            VARCHAR NOT NULL,
    RECORD_PK               NUMBER(38,0) NOT NULL,
    OPERATION               VARCHAR(1) NOT NULL,   -- c, u, d
    DATA_ORIGIN             VARCHAR,
    BUSINESS_DATE           DATE,
    AMOUNT                  NUMBER(12,2),
    INTENDED_COMMIT_AT      TIMESTAMP_TZ,
    ACTUAL_COMMIT_AT        TIMESTAMP_TZ,           -- NULL = planned, not (yet) committed
    INJECTED_DELAY_SECONDS  NUMBER(38,0),
    INJECTED_FAULT          VARCHAR,
    -- Start time of the sync that last wrote the row. Any change committed before the newest
    -- SYNCED_AT has its replay_event row here, which is what makes UNEXPECTED_CHANGE decidable.
    SYNCED_AT               TIMESTAMP_TZ NOT NULL
);

-- Reviewed losses. A MISSING change that an operator has investigated is recorded here and no
-- longer fails the build; it stays MISSING in AUDIT.RECON_REPLAY_DELIVERY with the reason.
-- REPLAY_EVENT_ID NULL acknowledges every change of the run committed before ACKNOWLEDGED_AT;
-- changes committed after the acknowledgement are never covered by it.
CREATE TABLE IF NOT EXISTS AUDIT.RECON_ACKNOWLEDGEMENT (
    REPLAY_RUN_ID     NUMBER(38,0) NOT NULL,
    REPLAY_EVENT_ID   NUMBER(38,0),
    REASON            VARCHAR NOT NULL,
    ACKNOWLEDGED_BY   VARCHAR NOT NULL,
    ACKNOWLEDGED_AT   TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);