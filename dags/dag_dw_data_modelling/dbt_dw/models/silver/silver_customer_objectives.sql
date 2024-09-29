WITH clean_data AS (
    SELECT 
        _id AS customerObjectiveId,
        customerId,
        staffId,
        objective AS objectiveId,
        CASE
        WHEN SAFE_CAST(REPLACE(startDate, ' UTC', '') AS DATETIME) IS NOT NULL
            AND SAFE_CAST(REPLACE(startDate, ' UTC', '') AS DATETIME) <= CURRENT_DATETIME()
            AND SAFE_CAST(REPLACE(startDate, ' UTC', '') AS DATE) >= DATE '2021-05-13'
        THEN SAFE_CAST(REPLACE(startDate, ' UTC', '') AS DATETIME)
        ELSE NULL
        END AS REPLACE(startDate, ' UTC', ''),
        CASE
        WHEN SAFE_CAST(REPLACE(createdAt, ' UTC', '') AS DATETIME) IS NOT NULL
            AND SAFE_CAST(REPLACE(createdAt, ' UTC', '') AS DATETIME) <= CURRENT_DATETIME()
            AND SAFE_CAST(REPLACE(createdAt, ' UTC', '') AS DATE) >= DATE '2021-05-13'
        THEN SAFE_CAST(REPLACE(createdAt, ' UTC', '') AS DATETIME)
        ELSE NULL
        END AS REPLACE(createdAt, ' UTC', ''),
        CASE
        WHEN SAFE_CAST(REPLACE(updatedAt, ' UTC', '') AS DATETIME) IS NOT NULL
            AND SAFE_CAST(REPLACE(updatedAt, ' UTC', '') AS DATETIME) <= CURRENT_DATETIME()
            AND SAFE_CAST(REPLACE(updatedAt, ' UTC', '') AS DATE) >= DATE '2021-05-13'
        THEN SAFE_CAST(REPLACE(updatedAt, ' UTC', '') AS DATETIME)
        ELSE NULL
        END AS REPLACE(updatedAt, ' UTC', '')
    FROM {{ ref('bronze_customer_objectives') }}
    WHERE 
        _id IS NOT NULL
        AND customerId IS NOT NULL
        AND SAFE_CAST(REPLACE(startDate, ' UTC', '') AS DATETIME) <= CURRENT_DATETIME() 
)

SELECT * FROM clean_data
