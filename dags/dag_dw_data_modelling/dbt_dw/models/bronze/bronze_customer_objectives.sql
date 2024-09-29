-- models/bronze/bronze_customer_objectives.sql
WITH raw_data AS (
  SELECT * FROM {{ source('raw_data', 'raw_tb_customer_objectives') }}
)

SELECT * FROM raw_data
