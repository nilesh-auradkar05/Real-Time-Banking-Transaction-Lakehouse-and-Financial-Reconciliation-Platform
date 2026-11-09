-- Faker rows must never be counted as original data, so every fact row needs a known origin.
select 'fact_transaction' as model, transaction_id::varchar as id from {{ ref('fact_transaction') }}
where data_origin = 'unknown' or data_origin_key is null
union all
select 'fact_loan', source_loan_id::varchar from {{ ref('fact_loan') }} where data_origin_key is null
