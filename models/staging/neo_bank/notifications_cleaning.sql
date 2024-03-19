SELECT * ,
FROM {{ ref('stg_neo_bank__notifications') }}
WHERE reason IS NOT NULL
  AND channel IS NOT NULL
  AND status IS NOT NULL
  AND user_id IS NOT NULL
  AND created_date IS NOT NULL
