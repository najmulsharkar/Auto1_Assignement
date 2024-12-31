/*
A LINK in Data Vault 2.0 captures relationships between two or more hubs, representing associations or transactions between business entities.
It stores the hash keys of connected hubs, a unique link hash key, load timestamp and source system.
*/

-- This configures the model as a view in the data warehouse Snowflake with a unique key
{{ config(
    materialized='view',
    unique_key=['PAYMENT_REQUEST__X__PAYMENT__X__PRIMER_ACCOUNT_HK']
) }}

WITH LINK_PAYMENT_REQUEST__X__PAYMENT__X__PRIMER_ACCOUNT AS (
    SELECT
        -- Generate a unique hash for the relationship among PAYMENT_REQUEST, PRIMER_ACCOUNT and PAYMENT
        MD5(CONCAT(
            MD5(PAYMENT_REQUEST_ID),
            MD5(PAYMENT_ID),
            MD5(PRIMER_ACCOUNT_ID)
        )) AS PAYMENT_REQUEST__X__PAYMENT__X__PRIMER_ACCOUNT_HK,

        -- hash keys of connected hubs
        MD5(PAYMENT_REQUEST_ID) AS PAYMENT_REQUEST_HK,
        MD5(PAYMENT_ID) AS PAYMENT_HK,
        MD5(PRIMER_ACCOUNT_ID) AS PRIMER_ACCOUNT_HK,
        
        CURRENT_TIMESTAMP AS LOAD_DATE, -- Current Timestamp for the load date
        'PAYMENT_REQUEST_DATA' AS RECORD_SOURCE
    FROM {{ ref('STAG_PAYMENT_REQUEST_DATA') }}
)

SELECT * FROM LINK_PAYMENT_REQUEST__X__PAYMENT__X__PRIMER_ACCOUNT
