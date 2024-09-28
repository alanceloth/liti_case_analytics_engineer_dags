-- models/bronze/bronze_weighins.sql
WITH raw_data AS (
  SELECT * FROM {{ source('raw_data', 'raw_weighins') }}
)

-- Aqui você pode aplicar qualquer transformação, caso necessário.
SELECT * FROM raw_data
