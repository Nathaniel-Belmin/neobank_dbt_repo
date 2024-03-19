select 
user_id, 
count(*) as nb_notifications_per_user
from  {{ ref('stg_neo_bank__notifications') }}  
group by user_id
