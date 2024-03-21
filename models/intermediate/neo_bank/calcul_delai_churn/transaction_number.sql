SELECT
    user_id,
    transaction_id,
    created_date,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_date) AS transaction_number,
    transactions_type,
    transactions_currency,
    amount_usd,
    transactions_state,
    transaction_direction,
 FROM {{ ref('stg_neo_bank__transactions') }}
  ORDER BY user_id, transaction_number