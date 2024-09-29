WITH clean_data AS (
  SELECT 
    _id AS customerMealPlanId,
    customerId,
    staffId,
    mealPlanId,
    CASE
      WHEN SAFE_CAST(startDate AS DATETIME) IS NOT NULL
           AND SAFE_CAST(startDate AS DATETIME) <= CURRENT_DATETIME()
           AND SAFE_CAST(startDate AS DATE) >= DATE '2021-05-13'
      THEN SAFE_CAST(startDate AS DATETIME)
      ELSE NULL
    END AS startDate,
    CASE
      WHEN SAFE_CAST(endDate AS DATETIME) IS NOT NULL
           AND SAFE_CAST(endDate AS DATETIME) <= CURRENT_DATETIME()
           AND SAFE_CAST(endDate AS DATE) >= DATE '2021-05-13'
      THEN SAFE_CAST(endDate AS DATETIME)
      ELSE NULL
    END AS endDate,
    restrictions_gluten AS glutenRestriction,
    restrictions_lactose AS lactoseRestriction,
    restrictions_vegan AS veganRestriction,
    restrictions_ovolacto AS ovolactoRestriction,
    restrictions_highFodMaps AS highFodMapsRestriction,
    CASE
      WHEN SAFE_CAST(createdAt AS DATETIME) IS NOT NULL
           AND SAFE_CAST(createdAt AS DATETIME) <= CURRENT_DATETIME()
           AND SAFE_CAST(createdAt AS DATE) >= DATE '2021-05-13'
      THEN SAFE_CAST(createdAt AS DATETIME)
      ELSE NULL
    END AS createdAt
  FROM {{ ref('bronze_customer_meal_plan') }}
  WHERE 
    _id IS NOT NULL
    AND customerId IS NOT NULL
    AND startDate <= CURRENT_DATETIME() 
)

SELECT * FROM clean_data
