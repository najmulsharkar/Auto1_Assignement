{{ config(
    materialized='view',
    unique_key=['PAYMENT_HK']
) }}

WITH SAT_PAYMENT AS (
    SELECT
        MD5(PAYMENT_ID) AS PAYMENT_HK,
        CURRENCY_CODE,
        CUSTOMER_ID,
        CUSTOMER_NAME,
        CUSTOMER_PHONE,
        CUSTOMER_EMAIL,
        CUSTOMER_BILLING_ADDRESS,
        CUSTOMER_SHIPPING_ADDRESS,
        STATEMENT_DESCRIPTOR,
        CREATED_AT,
        UPDATED_AT,
        VAULTED_TOKEN_ID,
        MERCHANT_PAYMENT_ID,
        AMOUNT,
        STATUS,
        PROCESSOR_MERCHANT_ID,
        PROCESSOR,
        AMOUNT_CAPTURED,
        AMOUNT_AUTHORIZED,
        AMOUNT_REFUNDED,
        UPDATED_AT AS EFFECTIVE_FROM,
        CREATED_AT AS LOAD_DATE,
        'PAYMENT_DATA' AS RECORD_SOURCE -- Source system name (Here table name for the simplicity)
    FROM {{ ref('STAG_PAYMENT_DATA') }}
)

SELECT * FROM SAT_PAYMENT
