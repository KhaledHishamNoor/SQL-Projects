{{ config(
    unique_key='customer_no',
    post_hook=["INSERT INTO {{source('metadata', 'kafka_deltas')}} SELECT  MAX(updated_at), 'aaml', 'sttm_customer' FROM  {{this}}"]
) }}

WITH deltas AS (
  SELECT COALESCE(MAX(delta_ts),0) AS latest_delta
  FROM  {{source('metadata', 'kafka_deltas')}}
  WHERE  project_name = 'aaml'
  AND  table_name = 'sttm_customer'
  )

SELECT  "CUSTOMER_NO"        AS customer_no,
        "FULL_NAME"          AS full_name,
        "CUSTOMER_TYPE"      AS customer_type,
        "CIF_CREATION_DATE"  AS cif_creation_date,
        "NATIONALITY"        AS nationality,
        "OP_TS"              AS updated_at
  FROM  {{ source('fcubs', 'sttm_customer') }}
 WHERE  "OP_TS" > (SELECT latest_delta FROM deltas)
