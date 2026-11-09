{{ config(materialized='table') }}
{{ cdc_scd2_history('fin_account', 'account_id') }}
