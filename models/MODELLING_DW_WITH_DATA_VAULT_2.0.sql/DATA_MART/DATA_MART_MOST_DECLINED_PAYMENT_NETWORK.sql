-- This configures the model as a view in the data warehouse Snowflake
{{ config(materialized='view') }}

-- Step 1: Selecting necessary tables first and columns needed
WITH LINK_PAYMENT__X__TOKEN AS (
    SELECT
        PAYMENT_HK,
        TOKEN_HK
    FROM
        {{ ref('RAW_CORE_LINK_PAYMENT__X__TOKEN__X__PRIMER_ACCOUNT') }}
),

SAT_PAYMENT AS (
    SELECT
        PAYMENT_HK,
        STATUS
    FROM
        {{ ref('RAW_CORE_SATELITE_PAYMENT') }}
),

SAT_PAYMENT_INSTRUCTION_TOKEN AS (
    SELECT
        TOKEN_HK,
        NETWORK
    FROM
        {{ ref('RAW_CORE_SATELITE_PAYMENT_INSTRUCTION_TOKEN') }}
),

-- Step 2: Joining link and sat to answer the questions
DECLINED_PAYMENT AS (
    SELECT
        PAYMENT_INSTRUCTION.NETWORK,
        -- Count the number of declined payments per network
        COUNT(*) AS NUMBER_OF_DECLINED_PAYMENT
    FROM
        LINK_PAYMENT__X__TOKEN
    INNER JOIN SAT_PAYMENT_INSTRUCTION_TOKEN AS PAYMENT_INSTRUCTION
        ON
            LINK_PAYMENT__X__TOKEN.TOKEN_HK
            = PAYMENT_INSTRUCTION.TOKEN_HK
    INNER JOIN
        SAT_PAYMENT AS PAYMENT
        ON
            LINK_PAYMENT__X__TOKEN.PAYMENT_HK
            = PAYMENT.PAYMENT_HK
    WHERE
        PAYMENT.STATUS = 'DECLINED' -- Filter only rows with a 'DECLINED' status
    GROUP BY
        PAYMENT_INSTRUCTION.NETWORK -- Group by network to count declined payments per network
    ORDER BY
        NUMBER_OF_DECLINED_PAYMENT DESC -- Sort in descending order of the number of declined payments
)

-- Step 3: Retrieve the network with the highest number of declined payments
SELECT
    NETWORK,
    NUMBER_OF_DECLINED_PAYMENT
FROM DECLINED_PAYMENT
LIMIT 1

-- AMEX network has the highest number (57) of declined payments
