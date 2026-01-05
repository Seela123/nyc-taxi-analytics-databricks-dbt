{{ config (materialized = 'table')}}

SELECT
    pulocationid,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes
FROM {{ ref('silver_yellow_data') }}
WHERE total_amount > 0
GROUP BY pulocationid
