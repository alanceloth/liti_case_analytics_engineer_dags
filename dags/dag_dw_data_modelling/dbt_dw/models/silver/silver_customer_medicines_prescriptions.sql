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
    CAST(createdAt AS DATETIME) AS createdAt,
    CAST(updatedAt AS DATETIME) AS updatedAt
  FROM {{ ref('bronze_customer_medicines_prescriptions') }}
  WHERE 
    _id IS NOT NULL
    AND customerId IS NOT NULL
    AND createdAt <= CURRENT_DATETIME()
)

SELECT * FROM clean_data;
