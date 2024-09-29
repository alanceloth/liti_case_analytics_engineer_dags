WITH clean_data AS (
  SELECT 
    CustomerId,
    
    -- Tratamento de customerGender
    COALESCE(customerGender, 'unknown') AS customerGender,
    
    -- Validação de customerHeight
    CASE 
      WHEN customerHeight BETWEEN 50 AND 300 THEN customerHeight 
      ELSE NULL 
    END AS customerHeight,
    
    -- Validação de customerBirthDate (idade <= 110 anos)
    CASE
      WHEN SAFE_CAST(customerBirthDate AS DATE) IS NOT NULL
           AND DATE_DIFF(CURRENT_DATE(), SAFE_CAST(customerBirthDate AS DATE), YEAR) <= 110
      THEN SAFE_CAST(customerBirthDate AS DATE)
      ELSE NULL
    END AS customerBirthDate,
    
    originChannelGroup,
    customerPlan,
    isActive,
    isActivePaid,
    
    -- Validação de firstStartDate (não posterior à data atual e não anterior a 2021-05-13)
    CASE
      WHEN SAFE_CAST(firstStartDate AS DATE) IS NOT NULL
           AND SAFE_CAST(firstStartDate AS DATE) >= DATE '2021-05-13'
           AND SAFE_CAST(firstStartDate AS DATE) <= CURRENT_DATE()
      THEN SAFE_CAST(firstStartDate AS DATE)
      ELSE NULL
    END AS firstStartDate,
    
    -- Validação de acquiredDate (não posterior à data atual e não anterior a 2021-05-13)
    CASE
      WHEN SAFE_CAST(acquiredDate AS DATE) IS NOT NULL
           AND SAFE_CAST(acquiredDate AS DATE) >= DATE '2021-05-13'
           AND SAFE_CAST(acquiredDate AS DATE) <= CURRENT_DATE()
      THEN SAFE_CAST(acquiredDate AS DATE)
      ELSE NULL
    END AS acquiredDate,
    
    -- Validação de firstPaymentDate (não posterior à data atual e não anterior a 2021-05-13)
    CASE
      WHEN SAFE_CAST(firstPaymentDate AS DATE) IS NOT NULL
           AND SAFE_CAST(firstPaymentDate AS DATE) <= CURRENT_DATE()
           AND SAFE_CAST(firstPaymentDate AS DATE) >= DATE '2021-05-13'
      THEN SAFE_CAST(firstPaymentDate AS DATE)
      ELSE NULL
    END AS firstPaymentDate,
    
    -- Validação de lastChargePaidDate (não posterior à data atual e não anterior a 2021-05-13)
    CASE
      WHEN SAFE_CAST(lastChargePaidDate AS DATE) IS NOT NULL
           AND SAFE_CAST(lastChargePaidDate AS DATE) <= CURRENT_DATE()
           AND SAFE_CAST(lastChargePaidDate AS DATE) >= DATE '2021-05-13'
      THEN SAFE_CAST(lastChargePaidDate AS DATE)
      ELSE NULL
    END AS lastChargePaidDate,
    
    -- Validação de churnDate (não anterior a 2021-05-13)
    CASE 
      WHEN isActive = FALSE AND SAFE_CAST(churnDate AS DATE) IS NOT NULL
           AND SAFE_CAST(churnDate AS DATE) >= DATE '2021-05-13'
      THEN SAFE_CAST(churnDate AS DATE)
      ELSE NULL 
    END AS churnDate,
    
    customerInOnboarding,
    customerDoctor,
    customerNutritionist,
    customerBesci,
    
    -- Validação de customerCreatedAt (não posterior à data atual e não anterior a 2021-05-13)
    CASE
    WHEN SAFE_CAST(TIMESTAMP(customerCreatedAt) AS DATETIME) IS NOT NULL
         AND SAFE_CAST(TIMESTAMP(customerCreatedAt) AS DATETIME) <= CURRENT_DATETIME()
         AND SAFE_CAST(TIMESTAMP(customerCreatedAt) AS DATE) >= DATE '2021-05-13'
    THEN SAFE_CAST(TIMESTAMP(customerCreatedAt) AS DATETIME)
    ELSE NULL
    END AS customerCreatedAt
  FROM {{ ref('bronze_customer') }}
  WHERE CustomerId IS NOT NULL
)

SELECT * FROM clean_data
