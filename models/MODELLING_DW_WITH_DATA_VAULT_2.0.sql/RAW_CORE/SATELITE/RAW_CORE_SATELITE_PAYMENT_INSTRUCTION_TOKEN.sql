/*
A SATELLITE in Data Vault 2.0 stores descriptive and historical data for a hub or link,
capturing changes over time along with metadata for tracking.
*/

-- This configures the model as a view in the data warehouse Snowflake with a unique key
{{ config(
    materialized='view',
    unique_key=['TOKEN_HK']
) }}

WITH SAT_PAYMENT_INSTRUCTION_TOKEN AS (
    SELECT
         -- Generate the hash key for the business key
        MD5(TOKEN_ID) AS TOKEN_HK,  -- HK = HASH KEY
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
