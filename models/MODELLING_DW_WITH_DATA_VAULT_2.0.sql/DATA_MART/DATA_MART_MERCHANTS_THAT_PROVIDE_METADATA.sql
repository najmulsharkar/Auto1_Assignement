-- This configures the model as a view in the data warehouse Snowflake
{{ config(materialized='view') }}

-- Step 1: Selecting necessary tables first and columns needed
WITH LINK_PAYMENT_REQUEST__X__PRIMER_ACCOUNT AS (
    SELECT
        PAYMENT_REQUEST_HK,
        PRIMER_ACCOUNT_HK
    FROM
        {{ ref('RAW_CORE_LINK_PAYMENT_REQUEST__X__PAYMENT__X__PRIMER_ACCOUNT') }}
),

SAT_PAYMENT_REQUEST AS (
    SELECT
        PAYMENT_REQUEST_HK,
        METADATA
    FROM
        {{ ref('RAW_CORE_SATELITE_PAYMENT_REQUEST') }}
),

SAT_PRIMER_ACCOUNT AS (
    SELECT
        PRIMER_ACCOUNT_HK,
        COMPANY_NAME
    FROM
        {{ ref('RAW_CORE_SATELITE_PRIMER_ACCOUNT') }}
),

-- Step 2: Joining with link and sat to answer the questions
MERCHANT_WITH_METADATA AS (
    SELECT
        PRIMER_ACCOUNT.COMPANY_NAME,
        PAYMENT_REQUEST.METADATA
    FROM
        LINK_PAYMENT_REQUEST__X__PRIMER_ACCOUNT AS LINK
        INNER JOIN SAT_PAYMENT_REQUEST AS PAYMENT_REQUEST
        ON
            LINK.PAYMENT_REQUEST_HK
            = PAYMENT_REQUEST.PAYMENT_REQUEST_HK
        LEFT JOIN SAT_PRIMER_ACCOUNT AS PRIMER_ACCOUNT
        ON
            LINK.PRIMER_ACCOUNT_HK
            = PRIMER_ACCOUNT.PRIMER_ACCOUNT_HK
    WHERE
        -- Include only rows where METADATA is not null
        PAYMENT_REQUEST.METADATA IS NOT NULL
)

-- Step 3: Selecting the distinct companies where metadata values provides
SELECT DISTINCT COMPANY_NAME FROM MERCHANT_WITH_METADATA
