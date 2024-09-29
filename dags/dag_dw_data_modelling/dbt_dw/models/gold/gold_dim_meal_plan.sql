SELECT 
  _id AS meal_plan_id,
  name AS meal_plan_name,
  group AS meal_plan_group,
  CAST(createdAt AS DATE) AS created_date
FROM {{ ref('silver_meal_plans') }}
