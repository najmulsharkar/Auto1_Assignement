WITH SOURCE_DATA_1 AS (
    SELECT DISTINCT -- Ensure unique business keys
        TOKEN_ID,
        CURRENT_TIMESTAMP AS LOAD_DATE,
        'PAYMENT_INSTRUCTION_TOKEN_DATA' AS RECORD_SOURCE -- Source system name
    FROM 
        {{ ref('STAG_PAYMENT_INSTRUCTION_TOKEN_DATA') }}
),

SOURCE_DATA_2 AS (
    SELECT DISTINCT -- Ensure unique business keys from the second source
        TOKEN_ID,
        CREATED_AT AS LOAD_DATE,
        'PAYMENT_DATA' AS RECORD_SOURCE -- Another source system
    FROM 
        {{ ref('STAG_PAYMENT_DATA') }}
),

COMBINED_SOURCE_DATA AS (
    -- Combine unique TOKEN_IDs from both sources
    SELECT * FROM SOURCE_DATA_1
    UNION ALL
    SELECT * FROM SOURCE_DATA_2
),

DISTINCT_SOURCE_DATA AS (
    -- Deduplicate TOKEN_IDs to avoid duplicates in the hub
    SELECT DISTINCT 
        TOKEN_ID,
        MIN(LOAD_DATE) AS LOAD_DATE, -- Use the earliest load date
        MIN(RECORD_SOURCE) AS RECORD_SOURCE -- Just Picking one source for the record
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
