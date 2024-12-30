-- Step 1: Calculate the number of declined payments grouped by network

WITH DECLINED_PAYMENT AS (
    SELECT
        PAYMENT_INSTRUCTION.NETWORK,
        -- Count the number of declined payments per network
        COUNT(*) AS NUMBER_OF_DECLINED_PAYMENT
    FROM 
        {{ ref('STAG_PAYMENT_INSTRUCTION_TOKEN_DATA') }} AS PAYMENT_INSTRUCTION
    INNER JOIN
        {{ ref('STAG_PAYMENT_DATA') }} AS PAYMENT
    USING(TOKEN_ID)
    WHERE
        PAYMENT.STATUS = 'DECLINED' -- Filter only rows with a 'DECLINED' status
    GROUP BY
        PAYMENT_INSTRUCTION.NETWORK -- Group by network to count declined payments per network
    ORDER BY
        NUMBER_OF_DECLINED_PAYMENT DESC -- Sort in descending order of the number of declined payments
)

-- Step 2: Retrieve the network with the highest number of declined payments
SELECT
    NETWORK,
    NUMBER_OF_DECLINED_PAYMENT
FROM DECLINED_PAYMENT
LIMIT 1

-- AMEX network has the highest number (57) of declined payments
