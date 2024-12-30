WITH MERCHANT_WITH_METADATA AS (
    SELECT
        PRIMER_ACCOUNT.COMPANY_NAME,
        PAYMENT_REQUEST.METADATA
    FROM
        {{ ref('STAG_PRIMER_ACCOUNT') }} AS PRIMER_ACCOUNT
    LEFT JOIN
        {{ ref('STAG_PAYMENT_REQUEST_DATA') }} AS PAYMENT_REQUEST
    USING(PRIMER_ACCOUNT_ID) --Using PRIMER_ACCOUNT_ID to join both tables
    WHERE
        -- Include only rows where METADATA is not null
        PAYMENT_REQUEST.METADATA IS NOT NULL
)

--Selecting the distinct companies where metadata values provides
SELECT DISTINCT COMPANY_NAME FROM MERCHANT_WITH_METADATA
