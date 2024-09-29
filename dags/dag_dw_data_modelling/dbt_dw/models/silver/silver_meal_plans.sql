WITH clean_data AS (
  SELECT 
    _id AS mealPlanId,
    name,
    CASE 
      WHEN `group` IN ('Padrão', 'Atleta', 'Vegano', 'Vegetariano', 'Hipertrofia') THEN `group`
      ELSE NULL 
    END AS `group`,
    CASE
        WHEN SAFE_CAST(TIMESTAMP(createdAt) AS DATETIME) IS NOT NULL
            AND SAFE_CAST(TIMESTAMP(createdAt) AS DATETIME) <= CURRENT_DATETIME()
            AND SAFE_CAST(TIMESTAMP(createdAt) AS DATE) >= DATE '2021-05-13'
        THEN SAFE_CAST(TIMESTAMP(createdAt) AS DATETIME)
        ELSE NULL
    END AS createdAt
  FROM {{ ref('bronze_meal_plans') }}
  WHERE 
    _id IS NOT NULL
    AND name IS NOT NULL
    AND SAFE_CAST(TIMESTAMP(createdAt) AS DATETIME) <= CURRENT_DATETIME() 
)

SELECT * FROM clean_data
