-- models/bronze/bronze_customer_meal_plan.sql
WITH raw_data AS (
  SELECT * FROM {{ source('raw_data', 'raw_tb_customer_meal_plan') }}
)

SELECT * FROM raw_data
