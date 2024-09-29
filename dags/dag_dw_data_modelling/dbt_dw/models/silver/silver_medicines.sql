WITH clean_data AS (
  SELECT 
    _id AS medicineId,
    name,
    type,
    CASE 
      WHEN dosages_0_ = 'N/A' THEN NULL ELSE dosages_0_ END AS dosage_0,
    CASE 
      WHEN dosages_1_ = 'N/A' THEN NULL ELSE dosages_1_ END AS dosage_1,
    CASE 
      WHEN dosages_2_ = 'N/A' THEN NULL ELSE dosages_2_ END AS dosage_2,
    CASE 
      WHEN dosages_3_ = 'N/A' THEN NULL ELSE dosages_3_ END AS dosage_3,
    CASE 
      WHEN dosages_4_ = 'N/A' THEN NULL ELSE dosages_4_ END AS dosage_4,
    CASE 
      WHEN dosages_5_ = 'N/A' THEN NULL ELSE dosages_5_ END AS dosage_5,
    CASE 
      WHEN dosages_6_ = 'N/A' THEN NULL ELSE dosages_6_ END AS dosage_6,
    CASE 
      WHEN dosages_7_ = 'N/A' THEN NULL ELSE dosages_7_ END AS dosage_7,
    label,
    CASE
      WHEN deletedAt != '' 
           AND SAFE_CAST(TIMESTAMP(deletedAt) AS DATETIME) IS NOT NULL
           AND SAFE_CAST(TIMESTAMP(deletedAt) AS DATETIME) <= CURRENT_DATETIME()
           AND SAFE_CAST(TIMESTAMP(deletedAt) AS DATE) >= DATE '2021-05-13'
      THEN SAFE_CAST(TIMESTAMP(deletedAt) AS DATETIME)
      ELSE NULL
    END AS deletedAt
  FROM {{ ref('bronze_medicines') }}
  WHERE 
    _id IS NOT NULL
    AND name IS NOT NULL
    AND (deletedAt IS NULL OR deletedAt = '' OR SAFE_CAST(TIMESTAMP(deletedAt) AS DATETIME) <= CURRENT_DATETIME())
)

SELECT * FROM clean_data