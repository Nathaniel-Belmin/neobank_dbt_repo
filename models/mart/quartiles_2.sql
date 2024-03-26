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
        WHEN rt.quartile_by_transaction_amount = 1 THEN '1'
        WHEN rt.quartile_by_transaction_amount = 2 THEN '2'
        WHEN rt.quartile_by_transaction_amount = 3 THEN '3'
        WHEN rt.quartile_by_transaction_amount = 4 THEN '4'
        ELSE 'Unknown'
    END AS quartile_by_transaction_amount,
    rt.total_transactions,
    CASE
        WHEN rt.quartile_by_transaction_count = 1 THEN '1'
        WHEN rt.quartile_by_transaction_count = 2 THEN '2'
        WHEN rt.quartile_by_transaction_count = 3 THEN '3'
        WHEN rt.quartile_by_transaction_count = 4 THEN '4'
        ELSE 'Unknown'
    END AS quartile_by_transaction_count,
    lq.lifetime_month,
    CASE 
        WHEN lq.lifetime_quartile = 1 THEN '1'
        WHEN lq.lifetime_quartile = 2 THEN '2'
        WHEN lq.lifetime_quartile = 3 THEN '3'
        WHEN lq.lifetime_quartile = 4 THEN '4'
        ELSE 'Unknown'
    END AS lifetime_quartile
FROM
    ranked_transactions rt
JOIN 
    lifetime_quartiles lq 
    ON rt.user_id = lq.user_id
ORDER BY 
    rt.user_id