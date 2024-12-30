/*
This dbt model takes the raw data from the PAYMENT_DATA table, transforming and cleaning it to extract structured fields like customer details to make billing, and shipping addresses.
It prepares a cleaned dataset for downstream analysis while handling nested JSON structures and ensuring proper formatting of critical columns.
*/

{{ config(materialized='view') }}

WITH PAYMENT_DATA_TRANSFORMATION AS (
    SELECT
        PAYMENT_ID,
        CURRENCY_CODE,
        CUSTOMER_DETAILS,
        /*
        Replaces single quotes (') in the CUSTOMER_DETAILS column with double quotes (") to make the type JSON
        and then replaces the string 'None' to NULL
        After that extract customer-related fields from the nested JSON structure
        */
        PARSE_JSON(REPLACE(REPLACE(CUSTOMER_DETAILS, '''', '"'),'None','null')):customer_id::STRING AS CUSTOMER_ID,
        PARSE_JSON(REPLACE(REPLACE(CUSTOMER_DETAILS, '''', '"'),'None','null')):phone_number::NUMBER AS CUSTOMER_PHONE,
        PARSE_JSON(REPLACE(REPLACE(CUSTOMER_DETAILS, '''', '"'),'None','null')):email_address::STRING AS CUSTOMER_EMAIL,
        PARSE_JSON(REPLACE(REPLACE(CUSTOMER_DETAILS, '''', '"'),'None','null')):billing_address::STRING AS CUSTOMER_BILLING_ADDRESS,
        
        -- Parse customer first and last names from the JSON billing address
        PARSE_JSON(CUSTOMER_BILLING_ADDRESS):first_name::STRING AS CUSTOMER_FIRST_NAME,
        PARSE_JSON(CUSTOMER_BILLING_ADDRESS):last_name::STRING AS CUSTOMER_LAST_NAME,

        -- Parse detailed billing address components
        PARSE_JSON(CUSTOMER_BILLING_ADDRESS):address_line_1::STRING AS BILLING_STREET,
        PARSE_JSON(CUSTOMER_BILLING_ADDRESS):address_line_2::STRING AS BILLING_ADDITIONAL_ADDRESS,
        PARSE_JSON(CUSTOMER_BILLING_ADDRESS):city::STRING AS BILLING_CITY,
        PARSE_JSON(CUSTOMER_BILLING_ADDRESS):postal_code::STRING AS BILLING_POSTAL_CODE,
        PARSE_JSON(CUSTOMER_BILLING_ADDRESS):state::STRING AS BILLING_STATE,
        PARSE_JSON(CUSTOMER_BILLING_ADDRESS):country_code::STRING AS BILLING_COUNTRY_CODE,

        -- Parse detailed shipping address components
        PARSE_JSON(REPLACE(REPLACE(CUSTOMER_DETAILS, '''', '"'),'None','null')):shipping_address::STRING AS CUSTOMER_SHIPPING_ADDRESS,
        PARSE_JSON(CUSTOMER_SHIPPING_ADDRESS):address_line_1::STRING AS SHIPPING_STREET,
        PARSE_JSON(CUSTOMER_SHIPPING_ADDRESS):address_line_2::STRING AS SHIPPING_ADDITIONAL_ADDRESS,
        PARSE_JSON(CUSTOMER_SHIPPING_ADDRESS):city::STRING AS SHIPPING_CITY,
        PARSE_JSON(CUSTOMER_SHIPPING_ADDRESS):postal_code::STRING AS SHIPPING_POSTAL_CODE,
        PARSE_JSON(CUSTOMER_SHIPPING_ADDRESS):state::STRING AS SHIPPING_STATE,
        PARSE_JSON(CUSTOMER_SHIPPING_ADDRESS):country_code::STRING AS SHIPPING_COUNTRY_CODE,

        STATEMENT_DESCRIPTOR,
        CREATED_AT::TIMESTAMP_NTZ(9) AS CREATED_AT, --make sure it will be always timestamp no matter what source system provides the datatype
        UPDATED_AT::TIMESTAMP_NTZ(9) AS UPDATED_AT, --make sure it will be always timestamp no matter what source system provides the datatype
        TOKEN_ID,
        VAULTED_TOKEN_ID,
        MERCHANT_PAYMENT_ID,
        PRIMER_ACCOUNT_ID,
        AMOUNT,
        STATUS,
        PROCESSOR_MERCHANT_ID,
        PROCESSOR,
        AMOUNT_CAPTURED,
        AMOUNT_AUTHORIZED,
        AMOUNT_REFUNDED
    FROM
        {{ source('AUTO1_PROJECT', 'PAYMENT_DATA') }}
),

PAYMENT_DATA_CLEANED AS (
    SELECT
        PAYMENT_ID,
        CURRENCY_CODE,
        CUSTOMER_ID,
        -- Concatenate customer first and last names into a single column
        CONCAT(CUSTOMER_FIRST_NAME, ' ', CUSTOMER_LAST_NAME) AS CUSTOMER_NAME,
        CUSTOMER_PHONE,
        CUSTOMER_EMAIL,
        -- Constructs the CUSTOMER_BILLING_ADDRESS by concatenating billing details, ensuring each part is non-null.
        -- Returns NULL if either BILLING_STREET or BILLING_CITY is missing, as these are critical for a valid address.
        CASE
            WHEN BILLING_STREET IS NULL OR BILLING_CITY IS NULL THEN NULL
            ELSE
                CONCAT(
                    COALESCE(BILLING_STREET, ''), ', ',
                    COALESCE(BILLING_CITY, ''), ', ',
                    COALESCE(BILLING_POSTAL_CODE, ''), ', ',
                    COALESCE(BILLING_COUNTRY_CODE, '')
                )
        END AS CUSTOMER_BILLING_ADDRESS,
        -- Construct a formatted shipping address as like CUSTOMER_BILLING_ADDRESS
        CASE
            WHEN SHIPPING_STREET IS NULL OR SHIPPING_CITY IS NULL THEN NULL
            ELSE
                CONCAT(
                    COALESCE(SHIPPING_STREET, ''), ', ',
                    COALESCE(SHIPPING_CITY, ''), ', ',
                    COALESCE(SHIPPING_POSTAL_CODE, ''), ', ',
                    COALESCE(SHIPPING_COUNTRY_CODE, '')
                )
        END AS CUSTOMER_SHIPPING_ADDRESS,
        -- Include other columns
        STATEMENT_DESCRIPTOR,
        CREATED_AT,
        UPDATED_AT,
        TOKEN_ID,
        VAULTED_TOKEN_ID,
        MERCHANT_PAYMENT_ID,
        PRIMER_ACCOUNT_ID,
        AMOUNT,
        STATUS,
        PROCESSOR_MERCHANT_ID,
        PROCESSOR,
        AMOUNT_CAPTURED,
        AMOUNT_AUTHORIZED,
        AMOUNT_REFUNDED
    FROM PAYMENT_DATA_TRANSFORMATION
)

SELECT * FROM PAYMENT_DATA_CLEANED
