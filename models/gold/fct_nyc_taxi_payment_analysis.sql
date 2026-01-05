{{ config (materialized = 'table')}}


SELECT
    payment_type,
    COUNT(*) AS total_trip,
    SUM(total_amount) AS total_amount,
    AVG(tip_amount ) AS avg_tip_amount,
    AVG(tip_amount * 100.0 / NULLIF(fare_amount, 0)) * 100 AS avg_tip_percentage

FROM {{ ref('silver_yellow_data') }}
WHERE  total_amount > 0
GROUP BY




SELECT
    payment_type,
    COUNT(*) AS total_trip,
    SUM(total_amount) AS total_amount,
    ROUND(AVG(tip_amount), 2) AS avg_tip_amount,

    ROUND(
        SUM(tip_amount) / NULLIF(SUM(fare_amount), 0) * 100,
        2
    ) AS avg_tip_percentage

FROM {{ ref('silver_yellow_data') }}
WHERE fare_amount > 0
GROUP BY payment_type
