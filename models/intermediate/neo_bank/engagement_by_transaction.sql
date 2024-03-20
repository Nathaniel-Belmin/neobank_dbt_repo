WITH user_transactions AS (
    SELECT
        user_id,
        COUNT(DISTINCT transaction_id) AS num_transactions,
        MAX(created_date_transaction) AS max_created_date,
        MIN(created_date_transaction) AS min_created_date
    FROM {{ ref('merge_users_transactions_devices') }}
    GROUP BY user_id
),
--user lifetime equal the diff between account creation date and the date of the last transaction--
user_lifetime AS (
    SELECT
        user_id,
        DATE_DIFF(MAX(max_created_date), MIN(min_created_date), DAY) AS lifetime_days
    FROM user_transactions
    GROUP BY user_id
),
--baseline is the median number of transactions across all users
baseline AS (
    SELECT
        (SELECT APPROX_QUANTILES(num_transactions, 100)[OFFSET(50)] FROM user_transactions) AS median_transactions
),
-- the engagement status of each user based on their lifetime and transaction count compared to the median --
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

-- User Engagement Status = IF(Lifetime * Number of Transactions > Median Transactions, 'Engaged', 'Not Engaged')
--Explanation:
--Lifetime: The difference between the maximum and minimum transaction dates for a user, representing their lifetime in the system.
--Number of Transactions: The count of distinct transactions for a user.
--Median Transactions: The median number of transactions across all users.
--If the product of Lifetime and Number of Transactions for a user is greater than the Median Transactions, the user is considered "Engaged", otherwise "Not Engaged".