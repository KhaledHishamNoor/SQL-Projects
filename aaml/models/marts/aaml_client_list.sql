{{ config(
    materialized='incremental',
    unique_key='customer_no',
    incremental_strategy='merge'
) }}

SELECT  
    sc.customer_no                            AS customer_no,
    sc.full_name                              AS full_name,

    CASE sc.customer_type
        WHEN 'I' THEN 'INDIVIDUAL'
        WHEN 'C' THEN 'CORPORATE'
        WHEN 'B' THEN 'BANK'
        ELSE sc.customer_type
    END                                        AS customer_type,

    sc.cif_creation_date                      AS cif_creation_date,
    cfd.field_val_93                          AS adc_basetmgmt_ltd_customer,
    cfd.field_val_94                          AS on_boarding_date_to_adc_basetmgmt,

    CASE cfd.field_val_23
        WHEN '1' THEN '1-PREDEFINED NEUTRAL'
        WHEN '2' THEN '2-INCREASED'
        WHEN '3' THEN '3-UAE-PEP'
        WHEN '4' THEN '4-UN-ACCEPTABLE'
        WHEN '5' THEN '5-ASSESSED NEUTRAL'
        WHEN '6' THEN '6-NON-UAE PEP'
        WHEN '7' THEN '7-MEDIUM'
        WHEN '8' THEN '8-HIO PEP'
        ELSE cfd.field_val_23
    END                                        AS client_risk_classification,

    sc.nationality || '-' || scc.description  AS nationality,
    SUBSTR(scc1.host_ref_no, 1, 3)            AS onboarding_source,
    scp.d_country                             AS domicile_country,
    scpc.profession || '-' || scpc.profession_description AS profession,
    scpc1.profession                          AS profession_secondary,
    mcd.comp_mis_1 || '-' || gmc.code_desc    AS profit_center,

    (
        SELECT field_val_95
        FROM {{ ref('stg_cstm_function_userdef_fields') }}
        WHERE function_id = 'STDCIF'
          AND rec_key = sc.customer_no || '~'
        LIMIT 1
    )                                         AS last_kyc_review_date

FROM {{ ref('stg_sttm_customer') }} sc

JOIN {{ ref('stg_cstm_function_userdef_fields') }} cfd
  ON sc.customer_no = REPLACE(cfd.rec_key::text, '~', '')
 AND cfd.function_id = 'STDCIF'
 AND UPPER(cfd.field_val_93) = 'YES'

JOIN {{ ref('stg_sttm_country') }} scc
  ON sc.nationality = scc.country_code

JOIN {{ ref('stg_sttm_customer_custom') }} scc1
  ON sc.customer_no = scc1.customer_no

JOIN {{ ref('stg_sttm_cust_personal') }} scp
  ON sc.customer_no = scp.customer_no

JOIN {{ ref('stg_sttm_cust_personal_custom') }} scpc
  ON sc.customer_no = scpc.customer_no

JOIN {{ ref('stg_sttm_cust_profession_custom') }} scpc1
  ON sc.customer_no = scpc1.customer_no

JOIN {{ ref('stg_mitm_customer_default') }} mcd
  ON sc.customer_no = mcd.customer_no

JOIN {{ ref('stg_gltm_mis_code') }} gmc
  ON mcd.comp_mis_1 = gmc.mis_code
 AND gmc.mis_class = 'PROFT_CNT'

ORDER BY sc.cif_creation_date
