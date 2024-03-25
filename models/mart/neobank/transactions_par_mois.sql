WITH per_date AS (

SELECT 
    created_date,
    completed_transaction,
    COUNT(transaction_id) AS nbre_transactions,
    SUM(inbound_status) AS nbre_transactions_in,
    COUNT(inbound_status = 0) AS nbre_transactions_out,
    ROUND(SUM(amount_usd),2) AS tot_amount,
    ROUND(SUM(amount_inbound),2) AS tot_amount_in,
    ROUND(SUM(amount_outbound),2) AS tot_amount_out,
    COUNT(DISTINCT(user_id)) AS nbre_clients
FROM {{ ref('transactions_state_direction') }}
GROUP BY created_date, completed_transaction
HAVING completed_transaction = 1
),

per_trans AS (

    SELECT 
        created_date,
        tot_amount,
        nbre_transactions,
        ROUND(SAFE_DIVIDE(tot_amount, nbre_transactions),2) AS amount_per_trans,
        tot_amount_in,
        nbre_transactions_in,
        ROUND(SAFE_DIVIDE(tot_amount_in, nbre_transactions_in),2) AS amount_per_trans_in,
        tot_amount_out,
        nbre_transactions_out,
        ROUND(SAFE_DIVIDE(tot_amount_out, nbre_transactions_out),2) AS amount_per_trans_out,
        nbre_clients
    FROM per_date

)

SELECT
    created_date,
    nbre_clients,
    tot_amount,
    nbre_transactions,
    ROUND(SAFE_DIVIDE(nbre_transactions, nbre_clients),2) AS nbre_trans_per_client,
    amount_per_trans,
    ROUND(SAFE_DIVIDE(tot_amount, nbre_clients),2) AS amount_per_client,
    ROUND(SAFE_DIVIDE(amount_per_trans, nbre_clients),2) AS amount_per_trans_per_client,
    tot_amount_in,
    nbre_transactions_in,
    amount_per_trans_in,
    ROUND(SAFE_DIVIDE(tot_amount_in, nbre_clients),2) AS amount_IN_per_client,
    ROUND(SAFE_DIVIDE(amount_per_trans_in, nbre_clients),2) AS amount_per_trans_IN_per_client,
    tot_amount_out,
    nbre_transactions_out,
    amount_per_trans_out,
    ROUND(SAFE_DIVIDE(tot_amount_out, nbre_clients),2) AS amount_OUT_per_client,
    ROUND(SAFE_DIVIDE(amount_per_trans_out, nbre_clients),2) AS amount_per_trans_OUT_per_client
FROM per_trans



