WITH customer_dim AS (
  SELECT 
    CustomerId,
    customerGender,
    customerHeight,
    customerBirthDate,
    originChannelGroup
  FROM {{ ref('silver_customer') }}
),
weighins AS (
  SELECT
    _id,
    customerId,
    CAST(createdAt AS DATE) AS weighin_date,
    weight
  FROM {{ ref('silver_weighins') }}
)
SELECT
  w._id AS weighin_id,
  c.CustomerId,
  c.customerGender,
  c.customerHeight,
  c.customerBirthDate,
  c.originChannelGroup,
  w.weighin_date,
  w.weight
FROM weighins w
JOIN customer_dim c ON w.customerId = c.CustomerId
