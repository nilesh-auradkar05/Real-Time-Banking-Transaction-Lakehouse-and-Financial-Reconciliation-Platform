-- Source amounts, balances and banking dates must survive temporal/decimal parsing.
select trans_id from {{ ref('fin_trans') }}
where not is_deleted
  and (account_id is null or trans_date is null or amount is null or balance is null)
