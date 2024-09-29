WITH clean_data AS (
  SELECT 
    _id AS customerMealPlanId,
    customerId,
    staffId,
    mealPlanId,
    CASE
      WHEN SAFE_CAST(REPLACE(startDate, ' UTC', '') AS DATETIME) IS NOT NULL
           AND SAFE_CAST(REPLACE(startDate, ' UTC', '') AS DATETIME) <= CURRENT_DATETIME()
           AND SAFE_CAST(REPLACE(startDate, ' UTC', '') AS DATE) >= DATE '2021-05-13'
      THEN SAFE_CAST(REPLACE(startDate, ' UTC', '') AS DATETIME)
      ELSE NULL
    END AS REPLACE(startDate, ' UTC', ''),
    CASE
      WHEN SAFE_CAST(REPLACE(endDate, ' UTC', '') AS DATETIME) IS NOT NULL
           AND SAFE_CAST(REPLACE(endDate, ' UTC', '') AS DATETIME) <= CURRENT_DATETIME()
           AND SAFE_CAST(REPLACE(endDate, ' UTC', '') AS DATE) >= DATE '2021-05-13'
      THEN SAFE_CAST(REPLACE(endDate, ' UTC', '') AS DATETIME)
      ELSE NULL
    END AS REPLACE(endDate, ' UTC', ''),
    restrictions_gluten AS glutenRestriction,
    restrictions_lactose AS lactoseRestriction,
    restrictions_vegan AS veganRestriction,
    restrictions_ovolacto AS ovolactoRestriction,
    restrictions_highFodMaps AS highFodMapsRestriction,
    CASE
      WHEN SAFE_CAST(REPLACE(createdAt, ' UTC', '') AS DATETIME) IS NOT NULL
           AND SAFE_CAST(REPLACE(createdAt, ' UTC', '') AS DATETIME) <= CURRENT_DATETIME()
           AND SAFE_CAST(REPLACE(createdAt, ' UTC', '') AS DATE) >= DATE '2021-05-13'
      THEN SAFE_CAST(REPLACE(createdAt, ' UTC', '') AS DATETIME)
      ELSE NULL
    END AS REPLACE(createdAt, ' UTC', '')
  FROM {{ ref('bronze_customer_meal_plan') }}
  WHERE 
    _id IS NOT NULL
    AND customerId IS NOT NULL
    AND SAFE_CAST(REPLACE(startDate, ' UTC', '') AS DATETIME) <= CURRENT_DATETIME() 
)

SELECT * FROM clean_data
