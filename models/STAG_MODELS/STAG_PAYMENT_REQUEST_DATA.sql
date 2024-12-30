{{ config(materialized='view') }}

WITH PAYMENT_REQUEST_DATA_CLEANED AS (
    SELECT
        PAYMENT_REQUEST_ID,
        CURRENCY_CODE,
        CREATED_AT::TIMESTAMP_NTZ(9) AS CREATED_AT, --make sure it will be always timestamp no matter what source system provides the datatype
        PAYMENT_REQUEST_TYPE,
        PAYMENT_INSTRUMENT_VAULT_INTENTION,
        PAYMENT_ID,
        PRIMER_ACCOUNT_ID,
        METADATA,
        PAYMENT_INSTRUMENT_TOKEN_ID,
        AMOUNT,
        MERCHANT_REQUEST_ID
    FROM
        {{ source('AUTO1_PROJECT', 'PAYMENT_REQUEST_DATA') }}
)

SELECT * FROM PAYMENT_REQUEST_DATA_CLEANED
