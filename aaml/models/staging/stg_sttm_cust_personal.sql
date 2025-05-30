{{ config(
    unique_key='customer_no',
    post_hook=["INSERT INTO {{source('metadata', 'kafka_deltas')}} SELECT MAX(updated_at), 'aaml', 'sttm_cust_personal' FROM {{this}}"]
) }}

WITH deltas AS (
  SELECT COALESCE(MAX(delta_ts),0) AS latest_delta
  FROM {{source('metadata', 'kafka_deltas')}}
  WHERE project_name = 'aaml'
  AND table_name = 'sttm_cust_personal'
)

SELECT "CUSTOMER_NO" AS customer_no,
       "D_COUNTRY" AS d_country,
       "OP_TS" AS updated_at
FROM {{ source('fcubs', 'sttm_cust_personal') }}
WHERE "OP_TS" > (SELECT latest_delta FROM deltas)
