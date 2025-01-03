/*
A HUB in Data Vault 2.0 is a core table that stores unique business keys representing the primary
key in the data model (e.g., `CUSTOMER_ID`, `ORDER_ID`).
It contains minimal attributes: the business key, a unique hash key, load timestamp, and record source
*/

-- This configures the model as a view in the data warehouse Snowflake
{{ config(materialized='view') }}

WITH PAYMENT_REQUEST_DATA AS (
    SELECT DISTINCT -- Ensure unique business keys
        PAYMENT_REQUEST_ID, -- Business Key
        CREATED_AT AS LOAD_DATE,
        'PAYMENT_REQUEST_DATA' AS RECORD_SOURCE -- Source system name (Here table name for the simplicity)
    FROM 
        {{ ref('STAG_PAYMENT_REQUEST_DATA') }}
),

HUB_PAYMENT_REQUEST_DATA AS (
    SELECT
        -- Generate hash key from the business key
        MD5(PAYMENT_REQUEST_ID) AS PAYMENT_REQUEST_HK, -- HK = HASH KEY
        PAYMENT_REQUEST_ID,
        LOAD_DATE,
        RECORD_SOURCE
    FROM 
        PAYMENT_REQUEST_DATA
)
SELECT * FROM HUB_PAYMENT_REQUEST_DATA
