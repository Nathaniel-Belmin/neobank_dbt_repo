WITH transaction_status AS (

    SELECT *,

    CASE WHEN transactions_state = 'COMPLETED' THEN 1 ELSE 0 END AS completed_transaction,
    CASE WHEN transactions_state = 'COMPLETED' THEN amount_usd ELSE 0 END AS completed_transaction_amount,
    CASE WHEN transactions_state != 'COMPLETED' THEN amount_usd ELSE 0 END AS failed_transaction_amount,

FROM 
    {{ ref('stg_neo_bank__transactions') }}

) 

SELECT 
user_id,
transaction_id,
created_date,
transactions_type,
transactions_currency,
amount_usd,
completed_transaction, 
completed_transaction_amount,
failed_transaction_amount,
CASE WHEN completed_transaction = 1 AND transaction_direction = 'INBOUND' THEN 1 ELSE 0 END AS inbound_status,
CASE WHEN completed_transaction = 1 AND transaction_direction = 'INBOUND' THEN amount_usd ELSE 0 END AS amount_inbound,
CASE WHEN completed_transaction = 1 AND transaction_direction = 'OUTBOUND' THEN amount_usd ELSE 0 END AS amount_outbound


FROM transaction_status