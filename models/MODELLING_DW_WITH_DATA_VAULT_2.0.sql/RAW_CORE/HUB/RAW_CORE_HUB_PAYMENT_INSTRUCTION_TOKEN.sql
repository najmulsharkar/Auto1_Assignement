/*
A HUB in Data Vault 2.0 is a core table that stores unique business keys representing the primary
key in the data model (e.g., `CUSTOMER_ID`, `ORDER_ID`).
It contains minimal attributes: the business key, a unique hash key, load timestamp, and record source
*/

-- This configures the model as a view in the data warehouse Snowflake
{{ config(materialized='view') }}

WITH PAYMENT_INSTRUCTION_TOKEN_DATA AS (
    SELECT DISTINCT -- Ensure unique business keys
        TOKEN_ID, -- Business Key
        CURRENT_TIMESTAMP AS LOAD_DATE,
        'PAYMENT_INSTRUCTION_TOKEN_DATA' AS RECORD_SOURCE -- Source system name
    FROM 
        {{ ref('STAG_PAYMENT_INSTRUCTION_TOKEN_DATA') }}
),

PAYMENT_DATA AS (
    SELECT DISTINCT -- Ensure unique business keys from the second source
        TOKEN_ID, -- Business Key
        CREATED_AT AS LOAD_DATE,
        'PAYMENT_DATA' AS RECORD_SOURCE -- Another source system
    FROM 
        {{ ref('STAG_PAYMENT_DATA') }}
),

COMBINED_SOURCE_DATA AS (
    -- Combine unique TOKEN_IDs from both sources
    SELECT * FROM PAYMENT_INSTRUCTION_TOKEN_DATA
    UNION ALL
    SELECT * FROM PAYMENT_DATA
),

DISTINCT_SOURCE_DATA AS (
    -- Deduplicate TOKEN_IDs to avoid duplicates in the hub
    SELECT DISTINCT 
        TOKEN_ID,
        MIN(LOAD_DATE) AS LOAD_DATE, -- Use the earliest load date
        MIN(RECORD_SOURCE) AS RECORD_SOURCE -- Just Picking one source
    FROM
        COMBINED_SOURCE_DATA
    GROUP BY 
        TOKEN_ID
),

HUB_TOKEN_ID AS (
    SELECT
         -- Generate the hash key for the business key
        MD5(TOKEN_ID) AS TOKEN_HK, -- HK = HASH KEY
        TOKEN_ID,
        LOAD_DATE,
        RECORD_SOURCE
    FROM 
        DISTINCT_SOURCE_DATA
)
SELECT * FROM HUB_TOKEN_ID
