SELECT
    user_id,
    CASE
        WHEN DATE_DIFF(DATE '2019-05-16', MAX(created_date_transaction), DAY) > 120 THEN 'true'
        ELSE 'false'
    END AS churner
FROM {{ ref('merge_users_transactions_devices') }}
GROUP BY 
    user_id
