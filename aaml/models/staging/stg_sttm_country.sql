{{ config(
    unique_key='country_code',
    post_hook=["INSERT INTO {{source('metadata', 'kafka_deltas')}} SELECT MAX(updated_at), 'aaml', 'sttm_country' FROM {{this}}"]
) }}

WITH deltas AS (
  SELECT COALESCE(MAX(delta_ts),0) AS latest_delta
  FROM {{source('metadata', 'kafka_deltas')}}
  WHERE project_name = 'aaml'
  AND table_name = 'sttm_country'
)

SELECT "COUNTRY_CODE" AS country_code,
       "DESCRIPTION" AS description,
       "OP_TS" AS updated_at
FROM {{ source('fcubs', 'sttm_country') }}
WHERE "OP_TS" > (SELECT latest_delta FROM deltas)
