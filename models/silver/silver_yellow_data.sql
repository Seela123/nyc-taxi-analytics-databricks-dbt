{{ config(
    materialized = 'table'
) }}


SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    pulocationid,
    dolocationid,
    payment_type,
    ratecodeid,
    vendorid,
    {{ dbt.datediff('tpep_pickup_datetime', 'tpep_dropoff_datetime', 'minute') }} AS trip_duration_minutes


FROM

    {{ source('nyc_taxi','yellow_tripdata_bronz')}}

WHERE

    trip_distance > 0 AND
    passenger_count > 0 AND
    tpep_dropoff_datetime > tpep_pickup_datetime
