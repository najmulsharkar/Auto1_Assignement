-- Step 1: Calculate the daily count of successfully authorized payments for each processor

WITH DAILY_SUCESSFULL_AUTHORIZATION_PAYMENT AS (
    SELECT 
        PROCESSOR,
        UPDATED_AT::DATE AS PAYMENT_DATE, -- Ensure to group by the date without time
        COUNT(CASE
            WHEN STATUS IN ('AUTHORIZED', 'CANCELLED', 'SETTLING', 'SETTLED', 'PARTIALLY_SETTLED') 
            THEN 1 END)
        AS SUCESSFULL_PAYMENT -- Count payments with successful statuses
    FROM 
        {{ ref('STAG_PAYMENT_DATA') }}
    GROUP BY
    -- Group by processor and payment date to calculate daily totals
        PROCESSOR,
        PAYMENT_DATE
),

-- Step 2: Calculate the mean number of successful authorizations per processor per day

MEAN_SUCESSFULL_AUTHORIZATION_PAYMENT AS (
    SELECT 
        PROCESSOR,
        -- Calculate the average daily count
        AVG(SUCESSFULL_PAYMENT) AS MEAN_SUCESSFULL_AUTHORIZATION_PAYMENT_PER_DAY 
    FROM 
        DAILY_SUCESSFULL_AUTHORIZATION_PAYMENT
    GROUP BY 
        PROCESSOR -- Group by processor to compute the mean for each
)

-- Step 3: Select and display processors with their mean daily successful authorization counts, sorted in descending order

SELECT 
    PROCESSOR,
    MEAN_SUCESSFULL_AUTHORIZATION_PAYMENT_PER_DAY
FROM 
    MEAN_SUCESSFULL_AUTHORIZATION_PAYMENT
ORDER BY
    MEAN_SUCESSFULL_AUTHORIZATION_PAYMENT_PER_DAY DESC
