/*
A SATELLITE in Data Vault 2.0 stores descriptive and historical data for a hub or link,
capturing changes over time along with metadata for tracking.
*/

-- This configures the model as a view in the data warehouse Snowflake with a unique key
{{ config(
    materialized='view',
    unique_key=['PRIMER_ACCOUNT_HK']
) }}

WITH SAT_PRIMER_ACCOUNT AS (
    SELECT
        -- Generate the hash key for the business key
        MD5(PRIMER_ACCOUNT_ID) AS PRIMER_ACCOUNT_HK,  -- HK = HASH KEY
        COMPANY_NAME,
        CREATED_AT,
        CREATED_AT AS EFFECTIVE_FROM,
        CREATED_AT AS LOAD_DATE,
        'PRIMER_ACCOUNT' AS RECORD_SOURCE -- Source system name (Here table name for the simplicity)
    FROM {{ ref('STAG_PRIMER_ACCOUNT') }}
)

SELECT * FROM SAT_PRIMER_ACCOUNT
