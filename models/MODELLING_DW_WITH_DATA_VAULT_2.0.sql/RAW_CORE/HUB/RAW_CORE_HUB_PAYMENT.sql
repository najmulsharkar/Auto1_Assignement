WITH PAYMENT_DATA AS (
    SELECT DISTINCT -- Ensure unique business keys
        PAYMENT_ID,
        CREATED_AT AS LOAD_DATE,
        'PAYMENT_DATA' AS RECORD_SOURCE -- Source system name (Here table name for the simplicity)
    FROM 
        {{ ref('STAG_PAYMENT_DATA') }}
),

PAYMENT_REQUEST_DATA AS (
    SELECT DISTINCT -- Ensure unique business keys
        PAYMENT_ID,
        CURRENT_TIMESTAMP AS LOAD_DATE, -- current timestamp since this table does not have any date related column
        'PAYMENT_REQUEST_DATA' AS RECORD_SOURCE -- Source system name (Here table name for the simplicity)
    FROM 
        {{ ref('STAG_PAYMENT_REQUEST_DATA') }}
),

COMBINED_SOURCE_DATA AS (
    -- Combine unique PAYMENT_ID from both tables
    SELECT * FROM PAYMENT_DATA
    UNION ALL
    SELECT * FROM PAYMENT_REQUEST_DATA
),

DISTINCT_SOURCE_DATA AS (
    -- Deduplicate PAYMENT_ID to avoid duplicates in the hub
    SELECT DISTINCT 
        PAYMENT_ID,
        MIN(LOAD_DATE) AS LOAD_DATE, -- Use the earliest load date
        MIN(RECORD_SOURCE) AS RECORD_SOURCE -- Pick one source for the record
    FROM
        COMBINED_SOURCE_DATA
    GROUP BY 
        PAYMENT_ID
),

HUB_PAYMENT_ID AS (
    SELECT
        -- Generate hash key from the business key
        MD5(PAYMENT_ID) AS PAYMENT_HK, -- HK = HASH KEY
        PAYMENT_ID,
        LOAD_DATE,
        RECORD_SOURCE
    FROM 
        DISTINCT_SOURCE_DATA
)
SELECT * FROM HUB_PAYMENT_ID
