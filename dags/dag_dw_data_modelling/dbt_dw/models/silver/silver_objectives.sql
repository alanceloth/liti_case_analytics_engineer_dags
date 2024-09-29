WITH clean_data AS (
  SELECT 
    _id AS objectiveId,
    name,
    description,
    entity,
    key,
    display,
    objective,
    CASE 
      WHEN displayUnit IN ('kg', '%') THEN displayUnit 
      ELSE NULL 
    END AS displayUnit,
    CASE 
      WHEN type IN ('WEIGHT_LOSS', 'MUSCLE_GAIN', 'FAT_LOSS', 'CONSISTENCY') THEN type 
      ELSE NULL 
    END AS type
  FROM {{ ref('bronze_objectives') }}
  WHERE 
    _id IS NOT NULL
    AND name IS NOT NULL
    AND description IS NOT NULL
)

SELECT * FROM clean_data