SELECT
  country,
  COUNT(user_id) AS user_count
FROM {{ ref('users_cleaning') }}
GROUP BY country