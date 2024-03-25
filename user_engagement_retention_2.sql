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
)

SELECT *,
  DATE_DIFF(last_transaction_ever_month, first_transaction_ever_month, DAY) AS diff_last_to_first,
  DATE_DIFF(last_date_in_dataset, last_transaction_ever_month, DAY) AS days_since_last_order
FROM prepare