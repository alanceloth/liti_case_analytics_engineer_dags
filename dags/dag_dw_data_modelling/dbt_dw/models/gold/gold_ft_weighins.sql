WITH customer_dim AS (
  SELECT 
    CustomerId AS customer_id,
    customerGender AS customer_gender,
    customerHeight AS customer_height,
    customerBirthDate AS customer_birth_date,
    originChannelGroup AS origin_channel_group
  FROM {{ ref('silver_customer') }}
),
weighins AS (
  SELECT
    wheiginId AS weighin_id,
    owner_id AS customer_id,
    CAST(createdAt AS DATE) AS weighin_date,
    weightkg AS weight
  FROM {{ ref('silver_weighins') }}
)
SELECT
  w.weighin_id,
  c.customer_id,
  c.customer_gender,
  c.customer_height,
  c.customer_birth_date,
  c.origin_channel_group,
  w.weighin_date,
  w.weight
FROM weighins w
JOIN customer_dim c ON w.customer_id = c.customer_id
