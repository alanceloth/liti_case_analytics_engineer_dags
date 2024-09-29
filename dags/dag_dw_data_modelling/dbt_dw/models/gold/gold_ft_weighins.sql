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
    weighinId AS weighin_id,
    owner_id AS customer_id,
    CAST(createdAt AS DATE) AS weighin_date,
    weightkg AS weight,
    bodyFatPercent AS body_fat_percent,
    musclePercent AS muscle_percent,
    physicalAge AS physical_age,
    bmiStandard AS bmi,
    
    -- Calculando a variação de peso (em relação à pesagem anterior)
    LAG(weightkg, 1) OVER (PARTITION BY owner_id ORDER BY CAST(createdAt AS DATE)) AS previous_weight,
    weightkg - LAG(weightkg, 1) OVER (PARTITION BY owner_id ORDER BY CAST(createdAt AS DATE)) AS weight_variation
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
  w.weight,
  w.body_fat_percent,
  w.muscle_percent,
  w.physical_age,
  w.bmi,
  w.weight_variation
FROM weighins w
JOIN customer_dim c ON w.customer_id = c.customer_id
