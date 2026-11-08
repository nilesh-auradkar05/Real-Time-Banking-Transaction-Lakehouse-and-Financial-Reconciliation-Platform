-- Czech trans_type 'C' rows should not have a negative signed amount;
-- legitimate zeros are allowed. Flag disagreements without rewriting source.
select transaction_id from {{ ref('fact_transaction') }}
where trans_type='C' and amount < 0
