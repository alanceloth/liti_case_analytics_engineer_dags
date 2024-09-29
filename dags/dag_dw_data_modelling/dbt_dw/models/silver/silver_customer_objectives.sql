WITH clean_data AS (
    SELECT 
        _id AS customerObjectiveId,
        customerId,
        staffId,
        objective AS objectiveId,
        CAST(startDate AS DATETIME) AS startDate,
        CAST(createdAt AS DATETIME) AS createdAt,
        CAST(updatedAt AS DATETIME) AS updatedAt
    FROM {{ source('bronze_customer_objectives') }}
    WHERE 
        _id IS NOT NULL
        AND customerId IS NOT NULL
        AND startDate <= CURRENT_DATETIME()
)

SELECT * FROM clean_data;
