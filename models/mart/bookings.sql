SELECT
  unique_key as booking_id,
  to_date(date, 'YYYY-MM-DD')::DATE AS booking_date,
  metric_value,
  country_code,
  created_at,
  updated_at
FROM {{ source('bookings', 'source_bookings') }}