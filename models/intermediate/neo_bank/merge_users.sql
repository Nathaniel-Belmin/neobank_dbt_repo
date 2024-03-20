SELECT
    u.user_id,
    u.birth_year, 
    u.country,
    u.city,
    u.created_date AS created_date_account,
    u.settings_crypto_unlocked,
    u.plan,
    u.attributes_notifications_marketing_push,
    u.attributes_notifications_marketing_email,
    u.num_contacts,
    u.num_referrals,
    u.num_successful_referrals,
    a.avg_trans_nb_month,
    a.avg_trans_value_month,
    d.os
FROM
    {{ ref('users_cleaning') }} u
LEFT JOIN
    {{ ref('avg_nb_val_trans_per_month') }} a
USING(user_id)
LEFT JOIN
    {{ ref('stg_neo_bank__devices') }} d
USING(user_id)