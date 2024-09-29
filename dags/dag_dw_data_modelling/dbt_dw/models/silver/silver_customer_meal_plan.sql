WITH clean_data AS (
  SELECT 
    _id AS customerMealPlanId,
    customerId,
    staffId,
    mealPlanId,
    CAST(startDate AS DATETIME) AS startDate,
    CAST(endDate AS DATETIME) AS endDate,
    restrictions_gluten AS glutenRestriction,
    restrictions_lactose AS lactoseRestriction,
    restrictions_vegan AS veganRestriction,
    restrictions_ovolacto AS ovolactoRestriction,
    restrictions_highFodMaps AS highFodMapsRestriction,
    CAST(createdAt AS DATETIME) AS createdAt
  FROM {{ source('bronze_customer_meal_plan') }}
  WHERE 
    _id IS NOT NULL
    AND customerId IS NOT NULL
    AND startDate <= CURRENT_DATETIME() 
)

SELECT * FROM clean_data;
