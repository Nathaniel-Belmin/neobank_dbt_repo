SELECT
  reason,
  COUNT(*) AS nb_notifications
FROM {{ ref('stg_neo_bank__notifications') }}  
GROUP BY reason