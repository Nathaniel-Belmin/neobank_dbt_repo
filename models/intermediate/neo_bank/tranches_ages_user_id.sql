WITH distinct_id AS (

SELECT
  DISTINCT(user_id),
  birth_year
FROM {{ ref('merge_users_transactions_devices') }}
)
SELECT
  CASE
    WHEN 2019 - EXTRACT(YEAR FROM birth_year) BETWEEN 18 AND 24 THEN '18-24 ans'
    WHEN 2019 - EXTRACT(YEAR FROM birth_year) BETWEEN 25 AND 34 THEN '25-34 ans'
    WHEN 2019 - EXTRACT(YEAR FROM birth_year) BETWEEN 35 AND 44 THEN '35-44 ans'
    WHEN 2019 - EXTRACT(YEAR FROM birth_year) BETWEEN 45 AND 54 THEN '45-54 ans'
    WHEN 2019 - EXTRACT(YEAR FROM birth_year) BETWEEN 55 AND 64 THEN '55-64 ans'
    ELSE '65 ans et plus'
  END AS tranche_age,
  COUNT(user_id) AS nombre_utilisateurs
FROM distinct_id
GROUP BY tranche_age
ORDER BY tranche_age