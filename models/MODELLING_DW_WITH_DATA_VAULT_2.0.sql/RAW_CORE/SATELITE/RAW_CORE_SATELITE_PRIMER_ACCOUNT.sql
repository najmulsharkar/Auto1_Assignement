{{ config(
    materialized='view',
    unique_key=['PRIMER_ACCOUNT_HK']
) }}

WITH SAT_PRIMER_ACCOUNT AS (
    SELECT
        MD5(PRIMER_ACCOUNT_ID) AS PRIMER_ACCOUNT_HK,
        COMPANY_NAME,
        CREATED_AT,
        CREATED_AT AS EFFECTIVE_FROM,
        CREATED_AT AS LOAD_DATE,
        'PRIMER_ACCOUNT' AS RECORD_SOURCE -- Source system name (Here table name for the simplicity)
    FROM {{ ref('STAG_PRIMER_ACCOUNT') }}
)

SELECT * FROM SAT_PRIMER_ACCOUNT
