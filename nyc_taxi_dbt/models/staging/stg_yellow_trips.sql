{{ config(materialized='view') }}
SELECT
    -- Identification des courses
    SAFE_CAST(VendorID AS STRING) as vendor_id,
    -- Horodatage
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    -- Détails des courses
    SAFE_CAST(passenger_count AS INT64) as passenger_count, -- Utiliser SAFE_CAST
    SAFE_CAST(trip_distance AS FLOAT64) as trip_distance, -- Utiliser SAFE_CAST
    SAFE_CAST(RatecodeID AS INT64) as ratecode_id, -- Utiliser SAFE_CAST
    store_and_fwd_flag,
    SAFE_CAST(PULocationID AS INT64) as pickup_location_id, -- Utiliser SAFE_CAST
    SAFE_CAST(DOLocationID AS INT64) as dropoff_location_id, -- Utiliser SAFE_CAST
    SAFE_CAST(payment_type AS INT64) as payment_type_id, -- Utiliser SAFE_CAST
    -- Coûts
    SAFE_CAST(fare_amount AS FLOAT64) as fare_amount, -- Utiliser SAFE_CAST
    SAFE_CAST(extra AS FLOAT64) as extra, -- Utiliser SAFE_CAST
    SAFE_CAST(mta_tax AS FLOAT64) as mta_tax, -- Utiliser SAFE_CAST
    SAFE_CAST(tip_amount AS FLOAT64) as tip_amount, -- Utiliser SAFE_CAST
    SAFE_CAST(tolls_amount AS FLOAT64) as tolls_amount, -- Utiliser SAFE_CAST
    SAFE_CAST(improvement_surcharge AS FLOAT64) as improvement_surcharge, -- Utiliser SAFE_CAST
    SAFE_CAST(total_amount AS FLOAT64) as total_amount, -- Utiliser SAFE_CAST
    SAFE_CAST(congestion_surcharge AS FLOAT64) as congestion_surcharge, -- Utiliser SAFE_CAST
    -- Métadonnées
    EXTRACT(DATE FROM tpep_pickup_datetime) as pickup_date,
    EXTRACT(YEAR FROM tpep_pickup_datetime) as pickup_year,
    EXTRACT(MONTH FROM tpep_pickup_datetime) as pickup_month,
    EXTRACT(DAY FROM tpep_pickup_datetime) as pickup_day,
    EXTRACT(HOUR FROM tpep_pickup_datetime) as pickup_hour,
    EXTRACT(DAYOFWEEK FROM tpep_pickup_datetime) as pickup_weekday,
    -- Durée de la course
    TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, SECOND) as trip_duration_seconds,
    -- Arrondir à l'heure la plus proche pour faciliter le join avec les données météo horaires
    TIMESTAMP_TRUNC(tpep_pickup_datetime, HOUR) AS pickup_hour_rounded
FROM {{ source('nyc_taxi_data', 'trips') }}