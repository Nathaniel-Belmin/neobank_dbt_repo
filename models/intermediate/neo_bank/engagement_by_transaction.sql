WITH user_transactions AS (
    SELECT
        user_id,
        COUNT(DISTINCT transaction_id) AS num_transactions,
        MAX(created_date_transaction) AS max_created_date,
        MIN(created_date_transaction) AS min_created_date
    FROM {{ ref('merge_users_transactions_devices') }}
    GROUP BY user_id
),
user_lifetime AS (
    SELECT
        user_id,
        DATE_DIFF(MAX(max_created_date), MIN(min_created_date), DAY) AS lifetime_days
    FROM user_transactions
    GROUP BY user_id
),
baseline AS (
    SELECT
        (SELECT APPROX_QUANTILES(num_transactions, 100)[OFFSET(50)] FROM user_transactions) AS median_transactions
),
engagement_status AS (
    SELECT
        ul.user_id,
        ul.lifetime_days,
        COALESCE(ut.num_transactions, 0) AS num_transactions,
        CASE
            WHEN ul.lifetime_days * COALESCE(ut.num_transactions, 0) > (SELECT median_transactions FROM baseline) THEN 'Engaged'
            ELSE 'Not Engaged'
        END AS engagement_status
    FROM user_lifetime ul
    LEFT JOIN user_transactions ut ON ul.user_id = ut.user_id
)
SELECT *
FROM engagement_status
ORDER BY lifetime_days DESC
