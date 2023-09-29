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
    date_trunc('month', to_date(date, 'YYYY-MM-DD'))::DATE AS month,
    SUM(metric_value) AS monthly_metric
FROM {{ source('bookings', 'source_bookings') }}
GROUP BY month
ORDER BY month
