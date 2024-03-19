with 

source as (

    select * from {{ source('neo_bank', 'notifications')}}

),

notifications_clean as (

    select
   reason,
        channel,
        status,
        DATE(created_date) AS created_date,
        REGEXP_REPLACE(user_id, "user_","") as user_id
    from source)

select * from notifications_clean