select earlier.client_id from {{ ref('dim_client') }} earlier
join {{ ref('dim_client') }} later on earlier.client_id=later.client_id
 and earlier.client_key<>later.client_key
 and earlier.valid_from<coalesce(later.valid_to,'9999-12-31'::timestamp_ntz)
 and later.valid_from<coalesce(earlier.valid_to,'9999-12-31'::timestamp_ntz)
