-- models/bronze/bronze_medicines.sql
WITH raw_data AS (
  SELECT * FROM {{ source('raw_data', 'raw_tb_medicines') }}
)

SELECT * FROM raw_data
