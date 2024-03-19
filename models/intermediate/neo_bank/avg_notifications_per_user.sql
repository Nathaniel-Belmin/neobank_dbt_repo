SELECT AVG(nb_notifications_per_user) AS avg_notifications_per_user
FROM (
    SELECT COUNT(*) AS nb_notifications_per_user
    FROM {{ ref('stg_neo_bank__notifications') }} 
    GROUP BY user_id
) 
