-- models/bronze/bronze_customer.sql
WITH raw_data AS (
  SELECT * FROM {{ source('raw_data', 'raw_customer') }}
)

SELECT * FROM raw_data
