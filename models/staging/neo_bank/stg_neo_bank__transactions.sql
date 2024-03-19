with 

source as (

    select * from {{ source('neo_bank', 'transactions') }}

),

transactions as (

    select
        transaction_id,
        transactions_type,
        transactions_currency,
        amount_usd,
        transactions_state,
        ea_cardholderpresence,
        ea_merchant_mcc,
        ea_merchant_city,
        ea_merchant_country,
        direction,
        user_id,
        created_date

    from source

)

select 
    REGEXP_REPLACE(user_id, "user_","") as user_id,
    REGEXP_REPLACE(transaction_id, "transaction_","") as transaction_id,
    transactions_type,
    transactions_currency,
    amount_usd,
    transactions_state,
    ea_cardholderpresence as card_holder_presence,
    ea_merchant_mcc as merchant_mcc,
    ea_merchant_city as merchant_city,
    ea_merchant_country as merchant_country,
    direction as transaction_direction,
    DATE(created_date) as created_date

from transactions
