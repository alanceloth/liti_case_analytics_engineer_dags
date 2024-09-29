WITH customer_dim AS (
  SELECT 
    CustomerId AS customer_id,
    customerGender AS customer_gender,
    customerBirthDate AS customer_birth_date,
    originChannelGroup AS origin_channel_group
  FROM {{ ref('silver_customer') }}
),
medicines_dim AS (
  SELECT 
    medicineId AS medicine_id,
    name AS medicine_name,
    type AS medicine_type
  FROM {{ ref('silver_medicines') }}
),
prescriptions AS (
  SELECT
    customerMedicinesPrescriptionId AS prescription_id,
    customerId AS customer_id,
    staffId AS staff_id,
    medicineId AS medicine_id,
    dosage,
    description,
    CAST(createdAt AS DATE) AS prescription_date
  FROM {{ ref('silver_customer_medicines_prescriptions') }}
)
SELECT
  p.prescription_id,
  c.customer_id,
  c.customer_gender,
  c.customer_birth_date,
  c.origin_channel_group,
  m.medicine_name,
  m.medicine_type,
  p.dosage,
  p.description,
  p.prescription_date
FROM prescriptions p
JOIN customer_dim c ON p.customer_id = c.customer_id
JOIN medicines_dim m ON p.medicine_id = m.medicine_id
