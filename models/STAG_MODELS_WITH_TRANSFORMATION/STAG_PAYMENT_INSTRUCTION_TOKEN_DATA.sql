{{ config(materialized='view') }}

WITH PAYMENT_INSTRUCTION_TOKEN_DATA_TRANSFORMATION AS (
    SELECT
        TOKEN_ID,
        TOKEN_TYPE,
        THREE_D_SECURE_AUTHENTICATION,
        /*
        Replaces single quotes (') in the THREE_D_SECURE_AUTHENTICATION column with double quotes (") to make the type JSON
        and then replaces the string 'None' to NULL
        After that extract AUTHENTICATION related fields from the nested JSON structure
        */
        PARSE_JSON(REPLACE(REPLACE(THREE_D_SECURE_AUTHENTICATION, '''', '"'),'None','null')):reason_code::STRING AS REASON_CODE,
        PARSE_JSON(REPLACE(REPLACE(THREE_D_SECURE_AUTHENTICATION, '''', '"'),'None','null')):reason_text::STRING AS REASON_TEXT,
        PARSE_JSON(REPLACE(REPLACE(THREE_D_SECURE_AUTHENTICATION, '''', '"'),'None','null')):response_code::STRING AS RESPONSE_CODE,
        PARSE_JSON(REPLACE(REPLACE(THREE_D_SECURE_AUTHENTICATION, '''', '"'),'None','null')):challenge_issued::STRING AS CHALLNGE_ISSUED,
        PARSE_JSON(REPLACE(REPLACE(THREE_D_SECURE_AUTHENTICATION, '''', '"'),'None','null')):protocol_version::STRING AS PROTOCOL_VERSION,
        PAYMENT_INSTRUMENT_TYPE,
        NETWORK
    FROM
        {{ source('AUTO1_PROJECT', 'PAYMENT_INSTRUCTION_TOKEN_DATA') }}
)

SELECT
    TOKEN_ID,
    TOKEN_TYPE,
    REASON_CODE,
    REASON_TEXT,
    RESPONSE_CODE,
    CHALLNGE_ISSUED,
    PROTOCOL_VERSION,
    PAYMENT_INSTRUMENT_TYPE,
    NETWORK
FROM PAYMENT_INSTRUCTION_TOKEN_DATA_TRANSFORMATION
