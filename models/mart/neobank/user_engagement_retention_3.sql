WITH prepare AS (
  SELECT *,
    DATE_TRUNC(MIN(created_date_transaction) OVER(PARTITION BY user_id), MONTH) AS first_transaction_ever_month,
    DATE_TRUNC(MAX(created_date_transaction) OVER(PARTITION BY user_id), MONTH) AS last_transaction_ever_month,
    DATE_TRUNC(MIN(created_date_transaction) OVER(PARTITION BY user_id), QUARTER) AS first_transaction_ever_quarter,
    DATE_TRUNC(MAX(created_date_transaction) OVER(PARTITION BY user_id), QUARTER) AS last_transaction_ever_quarter,
    COUNT(transaction_id) OVER(PARTITION BY user_id) AS nb_transaction_total,
    SUM(amount_usd) OVER (PARTITION BY user_id) AS turnover_total,
    MAX(created_date_transaction) OVER() AS last_date_in_dataset
  FROM {{ ref('merge_users_transactions_devices') }}
),

cohort AS (

SELECT *,
  DATE_DIFF(last_transaction_ever_month, first_transaction_ever_month, MONTH) AS lifetime_month,
  DATE_DIFF(created_date_transaction, first_transaction_ever_month, MONTH) AS month_nb,
  DATE_DIFF(last_date_in_dataset, last_transaction_ever_month, DAY) AS days_since_last_order
FROM prepare
)

SELECT  *,
    CASE 
        WHEN DATE_DIFF('2019-05-16', last_transaction_ever_month, MONTH) > 3 THEN 1
        ELSE 0
    END AS churners,
FROM cohort