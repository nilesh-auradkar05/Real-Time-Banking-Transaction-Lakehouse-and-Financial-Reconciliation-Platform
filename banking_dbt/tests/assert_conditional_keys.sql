-- order_key / loan_key / card_key are -1 exactly when the transaction is not ROB / LO / CCW.
select transaction_id from {{ ref('fact_transaction') }}
where (order_key = -1) = (operation_code = 'ROB')
   or (loan_key = -1) = (category_code = 'LO')
   or (card_key = -1) = (operation_code = 'CCW')
   or order_key is null or loan_key is null or card_key is null
