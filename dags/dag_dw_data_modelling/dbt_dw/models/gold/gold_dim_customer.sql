SELECT 
  CustomerId,
  customerGender,
  customerHeight,
  customerBirthDate,
  originChannelGroup,
  customerPlan,
  isActive,
  acquiredDate
FROM {{ ref('silver_customer') }}
