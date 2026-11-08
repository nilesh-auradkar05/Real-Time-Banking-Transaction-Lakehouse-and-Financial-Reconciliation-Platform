select earlier.loan_id from {{ ref('dim_loan') }} earlier
join {{ ref('dim_loan') }} later on earlier.loan_id=later.loan_id
 and earlier.loan_key<>later.loan_key
 and earlier.valid_from<coalesce(later.valid_to,'9999-12-31'::timestamp_ntz)
 and later.valid_from<coalesce(earlier.valid_to,'9999-12-31'::timestamp_ntz)
