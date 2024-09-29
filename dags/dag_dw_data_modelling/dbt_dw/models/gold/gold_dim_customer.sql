SELECT 
  CustomerId AS customer_id,
  customerGender AS customer_gender,
  customerHeight AS customer_height,
  customerBirthDate AS customer_birth_date,
  originChannelGroup AS origin_channel_group,
  customerPlan AS customer_plan,
  isActive AS is_active,
  acquiredDate AS acquired_date
FROM {{ ref('silver_customer') }}
