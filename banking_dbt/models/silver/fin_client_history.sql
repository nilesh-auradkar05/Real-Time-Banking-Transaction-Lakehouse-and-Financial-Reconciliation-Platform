{{ config(materialized='table') }}
{{ cdc_scd2_history('fin_client', 'client_id') }}
