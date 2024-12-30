{{ config(materialized='view') }}

WITH PRIMER_ACCOUNT_CLEANED AS (
    SELECT
        PRIMER_ACCOUNT_ID,
        COMPANY_NAME,
        CREATED_AT::DATE AS CREATED_AT --make sure it will be always DATE type no matter what source system provides the datatype
    FROM
        {{ source('AUTO1_PROJECT', 'PRIMER_ACCOUNT') }}
)

SELECT * FROM PRIMER_ACCOUNT_CLEANED
