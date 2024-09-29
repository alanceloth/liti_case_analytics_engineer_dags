WITH clean_data AS (
  SELECT 
    _id AS mealPlanId,
    name,
    CASE 
      WHEN group IN ('Padrão', 'Atleta', 'Vegano', 'Vegetariano', 'Hipertrofia') THEN group 
      ELSE NULL 
    END AS group,
    CAST(createdAt AS DATETIME) AS createdAt
  FROM {{ ref('bronze_meal_plans') }}
  WHERE 
    _id IS NOT NULL
    AND name IS NOT NULL
    AND createdAt <= CURRENT_DATETIME() 
)

SELECT * FROM clean_data;
