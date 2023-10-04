SELECT
  unique_key,
  to_date(date, 'YYYY-MM-DD')::DATE AS booking_date,
  metric_value,
  country_code
FROM {{ source('bookings', 'source_bookings') }}