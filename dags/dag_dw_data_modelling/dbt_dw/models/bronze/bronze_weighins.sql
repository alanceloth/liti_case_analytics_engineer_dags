-- models/bronze/bronze_weighins.sql
WITH raw_data AS (
  SELECT * FROM {{ source('raw_data', 'raw_tb_weighins') }}
)

SELECT * FROM raw_data
