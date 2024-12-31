-- This configures the model as a view in the data warehouse Snowflake
{{ config(materialized='view') }}

SELECT
    PROCESSOR,
    -- Calculate the Authorization Rate as:
    -- (Number of Successfully Authorized Payments) divided by (Number of Non-Pending Payments)
    -- Multiply numerator by 1.0 to ensure the result is a decimal
    COUNT(CASE
        WHEN STATUS IN ('AUTHORIZED', 'CANCELLED', 'SETTLING', 'SETTLED', 'PARTIALLY_SETTLED') 
        THEN 1 END) * 1.0
        /
    COUNT(CASE
        WHEN STATUS != 'PENDING' THEN 1 END) AS AUTHORIZATION_RATE
FROM
    {{ ref('STAG_PAYMENT_DATA') }}
GROUP BY
    PROCESSOR -- Group by PROCESSOR to calculate the Authorization Rate for each processor
ORDER BY
    AUTHORIZATION_RATE DESC --Highest Authorization rate processors come first
