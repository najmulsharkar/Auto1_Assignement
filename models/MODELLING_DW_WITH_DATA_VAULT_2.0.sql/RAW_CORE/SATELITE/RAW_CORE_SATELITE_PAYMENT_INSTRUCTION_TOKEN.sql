{{ config(
    materialized='view',
    unique_key=['TOKEN_HK']
) }}

WITH SAT_PAYMENT_INSTRUCTION_TOKEN AS (
    SELECT
        MD5(TOKEN_ID) AS TOKEN_HK,
        TOKEN_TYPE,
        REASON_CODE,
        REASON_TEXT,
        RESPONSE_CODE,
        CHALLNGE_ISSUED,
        PROTOCOL_VERSION,
        PAYMENT_INSTRUMENT_TYPE,
        NETWORK,
        CURRENT_TIMESTAMP AS EFFECTIVE_FROM,
        CURRENT_TIMESTAMP AS LOAD_DATE,
        'PAYMENT_INSTRUCTION_TOKEN_DATA' AS RECORD_SOURCE -- Source system name (Here table name for the simplicity)
    FROM {{ ref('STAG_PAYMENT_INSTRUCTION_TOKEN_DATA') }}
)

SELECT * FROM SAT_PAYMENT_INSTRUCTION_TOKEN
