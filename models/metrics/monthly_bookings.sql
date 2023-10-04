{{
    config(
        meta={
            "datadrift":true,
            "datadrift_unique_key":"month",
            "datadrift_date":"month",
        }
    )
}}

SELECT 
    date_trunc('month', booking_date)::DATE AS month,
    ROUND(SUM(metric_value),2) AS monthly_booking_value
FROM {{ref('bookings')}}
GROUP BY month
ORDER BY month
