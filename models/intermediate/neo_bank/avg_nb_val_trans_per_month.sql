WITH engval AS (

SELECT
    user_id,
    COUNT(transaction_id) as nb_transaction,
    SUM(amount_usd) AS total_transactions_value,
    MIN(created_date_transaction) AS first_transaction_date,
    MAX(created_date_transaction) AS last_transaction_date
FROM `dbt-lewagon-project.dbt__intermediate.merge_users_transactions_devices`
GROUP BY user_id
),

calc_engval AS (

SELECT 
    user_id,
    nb_transaction,
    total_transactions_value,
    DATE_DIFF(last_transaction_date, first_transaction_date , month ) AS ltv_month
FROM engval
)

SELECT 
    user_id,
    ROUND(SAFE_DIVIDE(SUM(nb_transaction) , SUM(ltv_month)) , 2) AS avg_trans_nb_month,
    ROUND(SAFE_DIVIDE(SUM(total_transactions_value) , SUM(ltv_month)) ,2) AS avg_trans_value_month
FROM calc_engval
GROUP BY user_id