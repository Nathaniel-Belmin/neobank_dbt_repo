SELECT 
REGEXP_REPLACE(user_id, "user_","") as user_id,

PARSE_DATE('%Y', CAST(birth_year AS STRING)) AS birth_year,

CASE 
    WHEN country = 'IE' THEN 'Irlande'
    WHEN country = 'CH' THEN 'Suisse'
    WHEN country = 'DE' THEN 'Allemagne'
    WHEN country = 'CZ' THEN 'Tchéquie' 
    WHEN country = 'GB' THEN 'Grande-Bretagne'
    WHEN country = 'FR' THEN 'France'
    WHEN country = 'RO' THEN 'Roumanie'
    WHEN country = 'ES' THEN 'Espagne'
    WHEN country = 'HU' THEN 'Hongrie'
    WHEN country = 'GR' THEN 'Grèce'
    WHEN country = 'NL' THEN 'Pays-Bas'
    WHEN country = 'SI' THEN 'Slovénie'
    WHEN country = 'PL' THEN 'Pologne' 
    WHEN country = 'NO' THEN 'Norvège'
    WHEN country = 'MT' THEN 'Malte'
    WHEN country = 'SE' THEN 'Suède'
    WHEN country = 'PT' THEN 'Portugal'
    WHEN country = 'LT' THEN 'Lituanie' 
    WHEN country = 'FI' THEN 'Finlande'
    WHEN country = 'LV' THEN 'Lettonie'
    WHEN country = 'EE' THEN 'Estonie'
    WHEN country = 'AT' THEN 'Autriche'
    WHEN country = 'CY' THEN 'Chypre'
    WHEN country = 'HR' THEN 'Croatie'
    WHEN country = 'LU' THEN 'Luxembourg'
    WHEN country = 'BG' THEN 'Bulgarie' 
    WHEN country = 'BE' THEN 'Belgique'
    WHEN country = "IT" THEN 'Italie'
    WHEN country = 'JE' THEN 'Jersey'
    WHEN country = 'AU' THEN 'Australie'
    WHEN country = 'SK' THEN 'Slovaquie'
    WHEN country = 'LI' THEN 'Liechtenstein'
    WHEN country = 'IM' THEN 'Île de Man'
    WHEN country = 'IS' THEN 'Islande'
    WHEN country = 'GI' THEN 'Gibraltar'
    WHEN country = 'MQ' THEN 'Martinique'
    WHEN country = 'GF' THEN 'Guyane française'
    WHEN country = 'RE' THEN 'La Réunion'
    WHEN country = 'DK' THEN 'Danemark'
    WHEN country = 'GG' THEN 'Guernesey'
    WHEN country = 'GP' THEN 'Guadeloupe'
    ELSE country
END AS country,

city,

DATE(created_date) AS created_date,

CASE 
    WHEN user_settings_crypto_unlocked = 1 THEN 'true'
    WHEN user_settings_crypto_unlocked = 0 THEN 'false'
    ELSE 'unspecified'
  END AS settings_crypto_unlocked,

LOWER(plan) AS plan,

CASE 
    WHEN attributes_notifications_marketing_push = 1 THEN 'true'
    WHEN attributes_notifications_marketing_push = 0 THEN 'false'
    ELSE 'unspecified'
END AS attributes_notifications_marketing_push,

CASE 
    WHEN attributes_notifications_marketing_email = 1 THEN 'true'
    WHEN attributes_notifications_marketing_email = 0 THEN 'false'
    ELSE 'unspecified'
END AS attributes_notifications_marketing_email,

num_contacts,

num_referrals,

num_successful_referrals

FROM  {{source("neo_bank" , "users")}}