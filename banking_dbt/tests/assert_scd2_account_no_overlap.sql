select earlier.account_id from {{ ref('dim_account') }} earlier
join {{ ref('dim_account') }} later on earlier.account_id=later.account_id
 and earlier.account_key<>later.account_key
 and earlier.valid_from<coalesce(later.valid_to,'9999-12-31'::timestamp_ntz)
 and later.valid_from<coalesce(earlier.valid_to,'9999-12-31'::timestamp_ntz)
