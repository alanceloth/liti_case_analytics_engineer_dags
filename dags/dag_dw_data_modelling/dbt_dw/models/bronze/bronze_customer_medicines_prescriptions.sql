-- models/bronze/bronze_customer_medicines_prescriptions.sql
WITH raw_data AS (
  SELECT * FROM {{ source('raw_data', 'raw_tb_customer_medicines_prescriptions') }}
)

SELECT * FROM raw_data
