with 

source as (

    select * from {{ source('neo_bank', 'devices') }}

),

renamed as (

    select 
  
    string_field_0 AS OS,
    string_field_1 AS user_id,
    
  
  FROM {{source("neo_bank", "devices")}}

    

)

select * from renamed
