SELECT
    t.user_id,
    t.transaction_id,
    t.transactions_type,
    t.transactions_currency,
    t.amount_usd,
    t.transactions_state,
    t.card_holder_presence,
    t.merchant_mcc,
    t.merchant_city,
    t.merchant_country,
    t.transaction_direction,
    t.created_date AS created_date_transaction,
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
    d.os,
FROM
    {{ ref('stg_neo_bank__transactions') }} t
LEFT JOIN
    {{ ref('users_cleaning') }} u
USING(user_id)
LEFT JOIN
    {{ ref('stg_neo_bank__devices') }} d
USING(user_id)