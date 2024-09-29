WITH customer_dim AS (
  SELECT 
    CustomerId,
    customerGender,
    customerBirthDate,
    originChannelGroup
  FROM {{ ref('silver_customer') }}
),
medicines_dim AS (
  SELECT 
    _id AS medicine_id,
    name AS medicine_name,
    type AS medicine_type
  FROM {{ ref('silver_medicines') }}
),
prescriptions AS (
  SELECT
    _id AS prescription_id,
    customerId,
    staffId,
    medicineId,
    dosage,
    description,
    CAST(createdAt AS DATE) AS prescription_date
  FROM {{ ref('silver_customer_medicines_prescriptions') }}
)
SELECT
  p.prescription_id,
  c.CustomerId,
  c.customerGender,
  c.customerBirthDate,
  c.originChannelGroup,
  m.medicine_name,
  m.medicine_type,
  p.dosage,
  p.description,
  p.prescription_date
FROM prescriptions p
JOIN customer_dim c ON p.customerId = c.CustomerId
JOIN medicines_dim m ON p.medicineId = m.medicine_id;
