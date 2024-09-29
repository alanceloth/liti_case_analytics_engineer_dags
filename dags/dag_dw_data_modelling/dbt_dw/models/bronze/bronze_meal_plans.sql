-- models/bronze/bronze_meal_plans.sql
WITH raw_data AS (
  SELECT * FROM {{ source('raw_data', 'raw_tb_meal_plans') }}
)

SELECT * FROM raw_data
