{{ config(
    unique_key='rec_key',
    post_hook=["INSERT INTO {{source('metadata', 'kafka_deltas')}} SELECT MAX(updated_at), 'aaml', 'cstm_function_userdef_fields' FROM {{this}}"]
) }}

WITH deltas AS (
  SELECT COALESCE(MAX(delta_ts),0) AS latest_delta
  FROM {{source('metadata', 'kafka_deltas')}}
  WHERE project_name = 'aaml'
  AND table_name = 'cstm_function_userdef_fields'
)

SELECT "REC_KEY" AS rec_key,
       "FUNCTION_ID" AS function_id,
       "FIELD_VAL_23" AS field_val_23,
       "FIELD_VAL_93" AS field_val_93,
       "FIELD_VAL_94" AS field_val_94,
       "FIELD_VAL_95" AS field_val_95,
       "OP_TS" AS updated_at
FROM {{ source('fcubs', 'cstm_function_userdef_fields') }}
WHERE "OP_TS" > (SELECT latest_delta FROM deltas)
