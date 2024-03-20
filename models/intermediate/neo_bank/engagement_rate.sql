WITH user_transactions AS (
    SELECT
        user_id,
        plan,
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
        DATE_DIFF(MAX(max_created_date), MIN(min_created_date), MONTH) AS lifetime_months
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
        ROUND(SAFE_DIVIDE(num_transactions, lifetime_months), 2) as engagement_rate,
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

-- -- User Engagement Status = IF(Number of Transactions / Lifetime > Median Engagement Rate, 'Engaged', 'Not Engaged')
--Explanation:
--Lifetime: The difference between the maximum and minimum transaction dates for a user, representing their lifetime in the system.
--Number of Transactions: The count of distinct transactions for a user.
--Median Engagement Rate: The median Engagement Rate across all users.
--If the product of Lifetime and Number of Transactions for a user is greater than the Median Transactions, the user is considered "Engaged", otherwise "Not Engaged".