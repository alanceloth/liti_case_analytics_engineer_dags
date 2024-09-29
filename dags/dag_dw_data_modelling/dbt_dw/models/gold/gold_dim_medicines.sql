SELECT 
  medicineId AS medicine_id,
  name AS medicine_name,
  type AS medicine_type,
  label AS medicine_label
FROM {{ ref('silver_medicines') }}
