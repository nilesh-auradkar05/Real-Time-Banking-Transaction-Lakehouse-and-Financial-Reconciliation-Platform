{{ config(materialized='table') }}
{{ cdc_scd2_history('fin_loan', 'loan_id') }}
