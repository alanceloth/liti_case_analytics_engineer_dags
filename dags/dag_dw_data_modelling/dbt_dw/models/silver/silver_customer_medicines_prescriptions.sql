WITH clean_data AS (
  SELECT 
    _id AS customerMedicinesPrescriptionId,
    customerId,
    staffId,
    medicineId,
    dosage,
    CASE 
      WHEN description NOT IN ('N/A', 'XXX', 'sad') THEN description
      ELSE NULL 
    END AS description,
    CASE
        WHEN SAFE_CAST(REPLACE(createdAt, ' UTC', '') AS DATETIME) IS NOT NULL
            AND SAFE_CAST(REPLACE(createdAt, ' UTC', '') AS DATETIME) <= CURRENT_DATETIME()
            AND SAFE_CAST(REPLACE(createdAt, ' UTC', '') AS DATE) >= DATE '2021-05-13'
        THEN SAFE_CAST(REPLACE(createdAt, ' UTC', '') AS DATETIME)
        ELSE NULL
        END AS createdAt,
    CASE
        WHEN SAFE_CAST(REPLACE(updatedAt, ' UTC', '') AS DATETIME) IS NOT NULL
            AND SAFE_CAST(REPLACE(updatedAt, ' UTC', '') AS DATETIME) <= CURRENT_DATETIME()
            AND SAFE_CAST(REPLACE(updatedAt, ' UTC', '') AS DATE) >= DATE '2021-05-13'
        THEN SAFE_CAST(REPLACE(updatedAt, ' UTC', '') AS DATETIME)
        ELSE NULL
        END AS updatedAt
  FROM {{ ref('bronze_customer_medicines_prescriptions') }}
  WHERE 
    _id IS NOT NULL
    AND customerId IS NOT NULL
    AND SAFE_CAST(REPLACE(createdAt, ' UTC', '') AS DATETIME) <= CURRENT_DATETIME() 
)

SELECT * FROM clean_data
