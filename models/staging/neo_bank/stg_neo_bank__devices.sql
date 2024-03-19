with 

source as (
    select * from {{ source('neo_bank', 'devices') }}
),

devices_clean as (
    SELECT 
        OS,
        REGEXP_REPLACE(user_id, "user_", "") AS user_id
    FROM (
        SELECT
            string_field_0 AS OS,
            string_field_1 AS user_id
        FROM source
    )
)

select * from devices_clean
