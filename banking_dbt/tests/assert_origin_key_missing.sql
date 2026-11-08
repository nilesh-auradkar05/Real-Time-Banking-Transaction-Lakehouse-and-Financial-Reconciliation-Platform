select transaction_id from {{ ref('fact_transaction') }} where data_origin_key is null
