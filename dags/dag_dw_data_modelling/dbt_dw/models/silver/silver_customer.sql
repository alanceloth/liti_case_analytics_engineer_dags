WITH clean_data AS (
  SELECT 
    CustomerId,
    COALESCE(customerGender, 'unknown') AS customerGender,
    CASE 
      WHEN customerHeight BETWEEN 50 AND 300 THEN customerHeight 
      ELSE NULL 
    END AS customerHeight,
    CAST(customerBirthDate AS DATE) AS customerBirthDate,
    originChannelGroup,
    customerPlan,
    isActive,
    isActivePaid,
    CAST(firstStartDate AS DATE) AS firstStartDate,
    CAST(acquiredDate AS DATE) AS acquiredDate,
    CAST(firstPaymentDate AS DATE) AS firstPaymentDate,
    CAST(lastChargePaidDate AS DATE) AS lastChargePaidDate,
    CASE 
      WHEN isActive = false THEN churnDate 
      ELSE NULL 
    END AS churnDate,
    customerInOnboarding,
    customerDoctor,
    customerNutritionist,
    customerBesci,
    CAST(customerCreatedAt AS DATETIME) AS customerCreatedAt 
  FROM {{ ref('bronze_customer') }}
  WHERE CustomerId IS NOT NULL
)

SELECT * FROM clean_data
