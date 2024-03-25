WITH groupby AS (

SELECT 
    user_id,
    transactions_state,
    settings_crypto_unlocked,
    plan,
    attributes_notifications_marketing_push,
    attributes_notifications_marketing_email,
    MIN(birth_year) AS birth_year,
    MIN(created_date_account) AS client_creation_date,
    MIN(created_date_transaction) AS first_transaction_date,
    MAX(created_date_transaction) AS last_transaction_date,
    ROUND(SUM(amount_usd), 2) AS amount_usd
FROM {{ ref('merge_users_transactions_devices') }}
    GROUP BY user_id , transactions_state, settings_crypto_unlocked, plan, attributes_notifications_marketing_push, attributes_notifications_marketing_email
),

month_year AS (
SELECT
    user_id,
    transactions_state,
    settings_crypto_unlocked,
    plan,
    attributes_notifications_marketing_push,
    attributes_notifications_marketing_email,
    client_creation_date,
    EXTRACT( MONTH FROM client_creation_date) AS user_created_month,
    EXTRACT( YEAR FROM client_creation_date) AS user_created_year,
    first_transaction_date,
    EXTRACT( MONTH FROM first_transaction_date) AS first_transaction_month,
    EXTRACT( YEAR FROM first_transaction_date) AS first_transaction_year,
    last_transaction_date,
    EXTRACT( MONTH FROM last_transaction_date) AS last_transaction_month,
    EXTRACT( YEAR FROM last_transaction_date) AS last_transaction_year,
    amount_usd
FROM groupby
),

orderby AS(

SELECT
    user_id,
    transactions_state,
    settings_crypto_unlocked,
    plan,
    attributes_notifications_marketing_push,
    attributes_notifications_marketing_email,
    client_creation_date,
    CONCAT(user_created_year, '-M', user_created_month) AS user_creation_year_month,
    first_transaction_date,
    CONCAT(first_transaction_year, '-M', first_transaction_month) AS first_transaction_year_month,
    last_transaction_date,
    CONCAT(last_transaction_year, '-M', last_transaction_month) AS last_transaction_year_month,
    amount_usd
FROM month_year
)

SELECT
    user_id,
    transactions_state,
    settings_crypto_unlocked,
    plan,
    attributes_notifications_marketing_push,
    attributes_notifications_marketing_email,
    client_creation_date,
    CASE 
        WHEN user_creation_year_month = '2018-M1' THEN 'A-Janv2018'
        WHEN user_creation_year_month = '2018-M2' THEN 'B-Fevrier2018'
        WHEN user_creation_year_month = '2018-M3' THEN 'C-Mars2018'
        WHEN user_creation_year_month = '2018-M4' THEN 'D-Avril2018'
        WHEN user_creation_year_month = '2018-M5' THEN 'E-Mai2018'
        WHEN user_creation_year_month = '2018-M6' THEN 'F-Juin2018'
        WHEN user_creation_year_month = '2018-M7' THEN 'G-Juillet2018'
        WHEN user_creation_year_month = '2018-M8' THEN 'H-Aout2018'
        WHEN user_creation_year_month = '2018-M9' THEN 'I-Septembre2018'
        WHEN user_creation_year_month = '2018-M10' THEN 'J-Octobre2018'
        WHEN user_creation_year_month = '2018-M11' THEN 'K-Novembre2018'
        WHEN user_creation_year_month = '2018-M12' THEN 'L-Decembre2018'
        WHEN user_creation_year_month = '2019-M1' THEN 'M-Janv2019'
    END AS user_creation_yearmonth,
    first_transaction_date,
    CASE 
        WHEN first_transaction_year_month = '2018-M1' THEN 'A-Janv2018'
        WHEN first_transaction_year_month = '2018-M2' THEN 'B-Fevrier2018'
        WHEN first_transaction_year_month = '2018-M3' THEN 'C-Mars2018'
        WHEN first_transaction_year_month = '2018-M4' THEN 'D-Avril2018'
        WHEN first_transaction_year_month = '2018-M5' THEN 'E-Mai2018'
        WHEN first_transaction_year_month = '2018-M6' THEN 'F-Juin2018'
        WHEN first_transaction_year_month = '2018-M7' THEN 'G-Juillet2018'
        WHEN first_transaction_year_month = '2018-M8' THEN 'H-Aout2018'
        WHEN first_transaction_year_month = '2018-M9' THEN 'I-Septembre2018'
        WHEN first_transaction_year_month = '2018-M10' THEN 'J-Octobre2018'
        WHEN first_transaction_year_month = '2018-M11' THEN 'K-Novembre2018'
        WHEN first_transaction_year_month = '2018-M12' THEN 'L-Decembre2018'
        WHEN first_transaction_year_month = '2019-M1' THEN 'M-Janv2019'
    END AS first_transaction_yearmonth,
    last_transaction_date,
    CASE 
        WHEN last_transaction_year_month = '2018-M1' THEN 'A-Janvier2018'
        WHEN last_transaction_year_month = '2018-M2' THEN 'B-Fevrier2018'
        WHEN last_transaction_year_month = '2018-M3' THEN 'C-Mars2018'
        WHEN last_transaction_year_month = '2018-M4' THEN 'D-Avril2018'
        WHEN last_transaction_year_month = '2018-M5' THEN 'E-Mai2018'
        WHEN last_transaction_year_month = '2018-M6' THEN 'F-Juin2018'
        WHEN last_transaction_year_month = '2018-M7' THEN 'G-Juillet2018'
        WHEN last_transaction_year_month = '2018-M8' THEN 'H-Aout2018'
        WHEN last_transaction_year_month = '2018-M9' THEN 'I-Septembre2018'
        WHEN last_transaction_year_month = '2018-M10' THEN 'J-Octobre2018'
        WHEN last_transaction_year_month = '2018-M11' THEN 'K-Novembre2018'
        WHEN last_transaction_year_month = '2018-M12' THEN 'L-Decembre2018'
        WHEN last_transaction_year_month = '2019-M1' THEN 'M-Janvier2019'
        WHEN last_transaction_year_month = '2019-M2' THEN 'N-Fevrier2019'
        WHEN last_transaction_year_month = '2019-M3' THEN 'O-Mars2019'
        WHEN last_transaction_year_month = '2019-M4' THEN 'P-Avril2019'
        WHEN last_transaction_year_month = '2019-M5' THEN 'Q-Mai2019'
    END AS last_transaction_yearmonth,
    amount_usd
FROM orderby