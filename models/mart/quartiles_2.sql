WITH fl_transaction AS (
    SELECT 
        user_id,
        DATE_TRUNC(MIN(created_date), MONTH) AS first_transaction_date,
        DATE_TRUNC(MAX(created_date), MONTH) AS last_transaction_date
    FROM  {{ ref('stg_neo_bank__transactions') }}
    WHERE transactions_state = 'COMPLETED'
    GROUP BY user_id
),
lifetime AS (
    SELECT 
        user_id,
        DATE_DIFF(last_transaction_date, first_transaction_date, MONTH) AS lifetime_month
    FROM fl_transaction
),
lifetime_quartiles AS (
    SELECT 
        user_id,
        lifetime_month,
        NTILE(4) OVER (ORDER BY lifetime_month) AS lifetime_quartile
    FROM lifetime
),
ranked_transactions AS (
    SELECT
        user_id,
        SUM(amount_usd) AS total_transaction_amount,
        COUNT(transaction_id) AS total_transactions,
        NTILE(4) OVER (ORDER BY COUNT(transaction_id)) AS quartile_by_transaction_count,
        NTILE(4) OVER (ORDER BY SUM(amount_usd)) AS quartile_by_transaction_amount
    FROM
        {{ ref('stg_neo_bank__transactions') }}
    WHERE transactions_state = 'COMPLETED'
    GROUP BY
        user_id
)
SELECT 
    rt.user_id,
    rt.total_transaction_amount,
    CASE
        WHEN rt.quartile_by_transaction_amount = 1 THEN 'A. 0 et 400'
        WHEN rt.quartile_by_transaction_amount = 2 THEN 'B. 400 et 2171'
        WHEN rt.quartile_by_transaction_amount = 3 THEN 'C. 2171 et 6970'
        WHEN rt.quartile_by_transaction_amount = 4 THEN 'D. 6977 et 3206682'
        ELSE 'Unknown'
    END AS quartile_by_transaction_amount,
    rt.total_transactions,
    CASE
        WHEN rt.quartile_by_transaction_count = 1 THEN 'A. 1 et 13'
        WHEN rt.quartile_by_transaction_count = 2 THEN 'B. 13 et 50'
        WHEN rt.quartile_by_transaction_count = 3 THEN 'C. 50 et 143'
        WHEN rt.quartile_by_transaction_count = 4 THEN 'D. 143 et 4996'
        ELSE 'Unknown'
    END AS quartile_by_transaction_count,
    lq.lifetime_month,
    CASE 
        WHEN lq.lifetime_quartile = 1 THEN 'A. 0 et 4'
        WHEN lq.lifetime_quartile = 2 THEN 'B. 4 et 7 '
        WHEN lq.lifetime_quartile = 3 THEN 'C. 7 et 10'
        WHEN lq.lifetime_quartile = 4 THEN 'D. 10 et 16'
        ELSE 'Unknown'
    END AS lifetime_quartile
FROM
    ranked_transactions rt
JOIN 
    lifetime_quartiles lq 
    ON rt.user_id = lq.user_id
ORDER BY 
    rt.user_id