select
    cs.user_id,
    cs.churner,
    u.birth_year,
    u.country,
    u.city,
    u.created_date,
    u.settings_crypto_unlocked,
    u.attributes_notifications_marketing_email,
    u.attributes_notifications_marketing_push,
    u.num_contacts,
    uac.tranche_age,
    er.plan,
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