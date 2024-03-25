select
    cs.user_id,
    cs.churner,
    u.birth_year,
    uac.tranche_age,
    u.country,
    u.city,
    u.created_date,
    u.settings_crypto_unlocked,
    u.attributes_notifications_marketing_email,
    u.attributes_notifications_marketing_push,
    u.num_contacts,
    d.os, 
    CASE 
    WHEN er.plan = "metal_free" then "metal"
    WHEN er.plan = "premium_offer" then "premium"
    WHEN er.plan = "premium_free" then "premium"
    Else er.plan 
    END,
    er.lifetime_months,
    er.num_transactions,
    er.total_amount_usd,
    er.engagement_rate,
    er.amount_usd_per_lifetime,
from
   {{ ref('users_cleaning') }} as u 
inner join
    {{ ref('churn_status') }} as cs
using (user_id)
inner join
    {{ ref('user_id_age_category') }} as uac
using (user_id)
inner join
    {{ ref('engagement_rate_by_month_and_plan') }} as er
using (user_id)
inner join 
    {{ ref('stg_neo_bank__devices') }} as d
using (user_id)