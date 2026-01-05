{{ config(materialized = 'table') }}

WITH base AS (

    SELECT
        DATE(tpep_pickup_datetime) AS trip_date,
        total_amount,
        fare_amount,
        trip_distance,
        trip_duration_minutes
    FROM {{ ref('silver_yellow_data') }}
    WHERE total_amount > 0

)

SELECT
    trip_date,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(fare_amount) AS avg_fare_amount,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(trip_duration_minutes) AS avg_trip_duration_minutes
FROM base
GROUP BY trip_date
ORDER BY trip_date
