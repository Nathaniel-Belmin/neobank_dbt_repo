select channel, 
count(*) as nb_notifications_per_channel
from {{ ref('stg_neo_bank__notifications') }} 
group by channel