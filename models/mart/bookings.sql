{{
    config(
        meta={
            "datadrift":true,
            "datadrift_unique_key":"unique_key",
            "datadrift_date":"booking_date"
        }
    )
}}

SELECT
  unique_key,
  to_date(date, 'YYYY-MM-DD')::DATE AS booking_date,
  metric_value,
  country_code
FROM {{ source('bookings', 'source_bookings') }}