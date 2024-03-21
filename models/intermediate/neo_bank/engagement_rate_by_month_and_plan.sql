WITH user_transactions AS (
    SELECT
        user_id,
        plan,
        ROUND(SUM(CASE WHEN transactions_state = 'COMPLETED' THEN amount_usd ELSE 0 END), 2) AS total_amount_usd,
        COUNT(DISTINCT transaction_id) AS num_transactions,
        MAX(created_date_transaction) AS max_created_date,
        MIN(created_date_transaction) AS min_created_date
    FROM {{ ref('merge_users_transactions_devices') }}
    GROUP BY user_id, plan
),

user_lifetime AS (
    SELECT
        user_id,
        plan,
        DATE_DIFF(MAX(max_created_date), MIN(min_created_date), MONTH) + 1 AS lifetime_months
    FROM user_transactions
    GROUP BY user_id, plan
),

baseline AS (
    SELECT
        (SELECT APPROX_QUANTILES(SAFE_DIVIDE(num_transactions, lifetime_months), 100)[OFFSET(50)] FROM user_transactions, user_lifetime) AS median_transactions
),

engagement_status AS (
    SELECT
        ul.user_id,
        ul.plan,
        ul.lifetime_months,
        COALESCE(ut.num_transactions, 0) AS num_transactions,
        COALESCE(ut.total_amount_usd, 0) AS total_amount_usd,
        ROUND(SAFE_DIVIDE(ut.num_transactions, ul.lifetime_months), 2) as engagement_rate,
        ROUND(SAFE_DIVIDE(ut.total_amount_usd, ul.lifetime_months), 2) as amount_usd_per_lifetime,
        CASE
            WHEN SAFE_DIVIDE(ut.num_transactions, ul.lifetime_months) > (SELECT median_transactions FROM baseline) THEN 'Engaged'
            ELSE 'Not Engaged'
        END AS engagement_status
    FROM user_lifetime ul
    LEFT JOIN user_transactions ut ON ul.user_id = ut.user_id
)
SELECT *
FROM engagement_status
ORDER BY plan, engagement_rate
