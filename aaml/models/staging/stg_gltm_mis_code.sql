{{ config(
    unique_key='mis_code',
    post_hook=["INSERT INTO {{source('metadata', 'kafka_deltas')}} SELECT MAX(updated_at), 'aaml', 'gltm_mis_code' FROM {{this}}"]
) }}

WITH deltas AS (
  SELECT COALESCE(MAX(delta_ts),0) AS latest_delta
  FROM {{source('metadata', 'kafka_deltas')}}
  WHERE project_name = 'aaml'
  AND table_name = 'gltm_mis_code'
)

SELECT "MIS_CODE" AS mis_code,
       "MIS_CLASS" AS mis_class,
       "CODE_DESC" AS code_desc,
       "OP_TS" AS updated_at
FROM {{ source('fcubs', 'gltm_mis_code') }}
WHERE "OP_TS" > (SELECT latest_delta FROM deltas)
