{{ config(
    unique_key='customer_no',
    post_hook=["INSERT INTO {{source('metadata', 'kafka_deltas')}} SELECT MAX(updated_at), 'aaml', 'mitm_customer_default' FROM {{this}}"]
) }}

WITH deltas AS (
  SELECT COALESCE(MAX(delta_ts),0) AS latest_delta
  FROM {{source('metadata', 'kafka_deltas')}}
  WHERE project_name = 'aaml'
  AND table_name = 'mitm_customer_default'
)

SELECT "CUSTOMER" AS customer_no,
       "COMP_MIS_1" AS comp_mis_1,
       "OP_TS" AS updated_at
FROM {{ source('fcubs', 'mitm_customer_default') }}
WHERE "OP_TS" > (SELECT latest_delta FROM deltas)
