-- models/bronze/bronze_objectives.sql
WITH raw_data AS (
  SELECT * FROM {{ source('raw_data', 'raw_tb_objectives') }}
)

SELECT * FROM raw_data
