{{ config(
    materialized='view',
    unique_key=['PAYMENT_REQUEST_HK']
) }}

WITH SAT_PAYMENT_REQUEST AS (
    SELECT
        MD5(PAYMENT_REQUEST_ID) AS PAYMENT_REQUEST_HK,
        CURRENCY_CODE,
        CREATED_AT,
        PAYMENT_REQUEST_TYPE,
        PAYMENT_INSTRUMENT_VAULT_INTENTION,
        METADATA,
        PAYMENT_INSTRUMENT_TOKEN_ID,
        AMOUNT,
        MERCHANT_REQUEST_ID,
        CREATED_AT AS EFFECTIVE_FROM,
        CREATED_AT AS LOAD_DATE,
        'PAYMENT_REQUEST_DATA' AS RECORD_SOURCE -- Source system name (Here table name for the simplicity)
    FROM {{ ref('STAG_PAYMENT_REQUEST_DATA') }}
)

SELECT * FROM SAT_PAYMENT_REQUEST
