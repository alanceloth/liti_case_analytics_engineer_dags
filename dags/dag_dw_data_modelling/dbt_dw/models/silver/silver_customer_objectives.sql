WITH clean_data AS (
    SELECT 
        _id AS customerObjectiveId,
        customerId,
        staffId,
        objective AS objectiveId,
        CASE
        WHEN SAFE_CAST(startDate AS DATETIME) IS NOT NULL
            AND SAFE_CAST(startDate AS DATETIME) <= CURRENT_DATETIME()
            AND SAFE_CAST(startDate AS DATE) >= DATE '2021-05-13'
        THEN SAFE_CAST(startDate AS DATETIME)
        ELSE NULL
        END AS startDate,
        CASE
        WHEN SAFE_CAST(createdAt AS DATETIME) IS NOT NULL
            AND SAFE_CAST(createdAt AS DATETIME) <= CURRENT_DATETIME()
            AND SAFE_CAST(createdAt AS DATE) >= DATE '2021-05-13'
        THEN SAFE_CAST(createdAt AS DATETIME)
        ELSE NULL
        END AS createdAt,
        CASE
        WHEN SAFE_CAST(updatedAt AS DATETIME) IS NOT NULL
            AND SAFE_CAST(updatedAt AS DATETIME) <= CURRENT_DATETIME()
            AND SAFE_CAST(updatedAt AS DATE) >= DATE '2021-05-13'
        THEN SAFE_CAST(updatedAt AS DATETIME)
        ELSE NULL
        END AS updatedAt
    FROM {{ ref('bronze_customer_objectives') }}
    WHERE 
        _id IS NOT NULL
        AND customerId IS NOT NULL
        AND startDate <= CURRENT_DATETIME()
)

SELECT * FROM clean_data;
