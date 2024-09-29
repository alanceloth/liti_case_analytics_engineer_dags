WITH clean_data AS (
    SELECT 
        _id AS customerObjectiveId,
        customerId,
        staffId,
        objective AS objectiveId,
        CASE
        WHEN SAFE_CAST(TIMESTAMP(startDate) AS DATE) IS NOT NULL
            AND SAFE_CAST(TIMESTAMP(startDate) AS DATE) <= CURRENT_DATE()
            AND SAFE_CAST(TIMESTAMP(startDate) AS DATE) >= DATE '2021-05-13'
        THEN SAFE_CAST(TIMESTAMP(startDate) AS DATE)
        ELSE NULL
        END AS startDate,
        CASE
        WHEN SAFE_CAST(TIMESTAMP(createdAt) AS DATETIME) IS NOT NULL
            AND SAFE_CAST(TIMESTAMP(createdAt) AS DATETIME) <= CURRENT_DATETIME()
            AND SAFE_CAST(TIMESTAMP(createdAt) AS DATE) >= DATE '2021-05-13'
        THEN SAFE_CAST(TIMESTAMP(createdAt) AS DATETIME)
        ELSE NULL
        END AS createdAt,
        CASE
        WHEN SAFE_CAST(TIMESTAMP(updatedAt) AS DATETIME) IS NOT NULL
            AND SAFE_CAST(TIMESTAMP(updatedAt) AS DATETIME) <= CURRENT_DATETIME()
            AND SAFE_CAST(TIMESTAMP(updatedAt) AS DATE) >= DATE '2021-05-13'
        THEN SAFE_CAST(TIMESTAMP(updatedAt) AS DATETIME)
        ELSE NULL
        END AS updatedAt
    FROM {{ ref('bronze_customer_objectives') }}
    WHERE 
        _id IS NOT NULL
        AND customerId IS NOT NULL
        AND SAFE_CAST(TIMESTAMP(startDate) AS DATE) <= CURRENT_DATE() 
)

SELECT * FROM clean_data
