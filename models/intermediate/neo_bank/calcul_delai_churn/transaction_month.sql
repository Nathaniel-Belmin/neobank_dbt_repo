SELECT 
  user_id,
  created_month,
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_month) AS row_month
FROM (
  SELECT 
    user_id,
    FORMAT_DATE('%m', created_date) as created_month
  FROM {{ ref('transaction_number') }}
  WHERE EXTRACT(YEAR FROM created_date) = 2018
  GROUP BY user_id, FORMAT_DATE('%m', created_date) 
) AS subquery
ORDER BY user_id, created_month