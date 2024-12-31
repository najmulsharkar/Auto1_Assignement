{{ config(
    materialized='view',
    unique_key=['PAYMENT_REQUEST__X__PAYMENT__X__PRIMER_ACCOUNT_HK']
) }}

WITH LINK_PAYMENT_REQUEST__X__PAYMENT__X__PRIMER_ACCOUNT AS (
    SELECT
        -- Generate a unique hash for the relationship between PRIMER_ACCOUNT and PAYMENT
        MD5(CONCAT(
            MD5(PAYMENT_REQUEST_ID),
            MD5(PAYMENT_ID),
            MD5(PRIMER_ACCOUNT_ID)
        )) AS PAYMENT_REQUEST__X__PAYMENT__X__PRIMER_ACCOUNT_HK,

        -- Foreign key referencing the PAYMENT hub
        MD5(PAYMENT_REQUEST_ID) AS PAYMENT_REQUEST_HK,
        MD5(PAYMENT_ID) AS PAYMENT_HK,
        MD5(PRIMER_ACCOUNT_ID) AS PRIMER_ACCOUNT_HK,
        
        CURRENT_TIMESTAMP AS LOAD_DATE, -- Timestamp of data load
        'PAYMENT_REQUEST_DATA' AS RECORD_SOURCE -- Source system or process
    FROM {{ ref('STAG_PAYMENT_REQUEST_DATA') }} -- Replace with your staging table
)

SELECT * FROM LINK_PAYMENT_REQUEST__X__PAYMENT__X__PRIMER_ACCOUNT
