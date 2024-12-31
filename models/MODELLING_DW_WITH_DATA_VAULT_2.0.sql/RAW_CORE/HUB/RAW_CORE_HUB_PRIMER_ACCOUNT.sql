/*
A HUB in Data Vault 2.0 is a core table that stores unique business keys representing the primary
key in the data model (e.g., `CUSTOMER_ID`, `ORDER_ID`).
It contains minimal attributes: the business key, a unique hash key, load timestamp, and record source
*/

-- This configures the model as a view in the data warehouse Snowflake
{{ config(materialized='view') }}

WITH PRIMER_ACCOUNT AS (
    SELECT DISTINCT -- Ensure unique business keys
        PRIMER_ACCOUNT_ID, -- Business key
        CREATED_AT AS LOAD_DATE,
        'PRIMER_ACCOUNT' AS RECORD_SOURCE -- Source system name (Here table name for the simplicity)
    FROM 
        {{ ref('STAG_PRIMER_ACCOUNT') }}
),

PAYMENT_DATA AS (
    SELECT DISTINCT -- Ensure unique business keys
        PRIMER_ACCOUNT_ID, -- Business key
        CREATED_AT AS LOAD_DATE, -- current timestamp since this table does not have any date related column
        'PAYMENT_DATA' AS RECORD_SOURCE -- Source system name (Here table name for the simplicity)
    FROM 
        {{ ref('STAG_PAYMENT_DATA') }}
),

COMBINED_SOURCE_DATA AS (
    -- Combine unique PAYMENT_ID from both tables
    SELECT * FROM PRIMER_ACCOUNT
    UNION ALL
    SELECT * FROM PAYMENT_DATA
),

DISTINCT_SOURCE_DATA AS (
    -- Deduplicate PAYMENT_ID to avoid duplicates in the hub
    SELECT DISTINCT 
        PRIMER_ACCOUNT_ID,
        MIN(LOAD_DATE) AS LOAD_DATE, -- Use the earliest load date
        MIN(RECORD_SOURCE) AS RECORD_SOURCE -- Picking just one source
    FROM
        COMBINED_SOURCE_DATA
    GROUP BY 
        PRIMER_ACCOUNT_ID
),

HUB_PRIMER_ACCOUNT AS (
    SELECT
        -- Generate hash key from the business key
        MD5(PRIMER_ACCOUNT_ID) AS PRIMER_ACCOUNT_HK, -- HK = HASH KEY
        PRIMER_ACCOUNT_ID,
        LOAD_DATE,
        RECORD_SOURCE
    FROM 
        DISTINCT_SOURCE_DATA
)
SELECT * FROM HUB_PRIMER_ACCOUNT
