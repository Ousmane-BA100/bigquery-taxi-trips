{{ config(materialized='table') }}

SELECT
    -- Horodatages
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    
    -- Détails de la course
    SAFE_CAST(passenger_count AS INT64) AS passenger_count,
    SAFE_CAST(trip_distance AS FLOAT64) AS trip_distance,
    
    -- Tarifs
    SAFE_CAST(fare_amount AS FLOAT64) AS fare_amount,
    SAFE_CAST(tip_amount AS FLOAT64) AS tip_amount,
    SAFE_CAST(tolls_amount AS FLOAT64) AS tolls_amount,
    SAFE_CAST(total_amount AS FLOAT64) AS total_amount,
    
    -- Calculs
    TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, SECOND) AS trip_duration_seconds,
    TIMESTAMP_TRUNC(tpep_pickup_datetime, HOUR) AS pickup_hour_rounded,
    
    -- Indicateurs de qualité
    CASE 
        WHEN tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL THEN 1
        WHEN tpep_dropoff_datetime < tpep_pickup_datetime THEN 1
        WHEN trip_distance <= 0 THEN 1
        WHEN fare_amount <= 0 THEN 1
        ELSE 0 
    END AS has_data_quality_issues

FROM {{ source('raw_yellow_taxi_trips', 'raw_taxi_data') }}
WHERE 
    -- Filtre des données invalides
    tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND trip_distance IS NOT NULL
    AND trip_distance > 0
    AND fare_amount > 0
    AND pulocationid IS NOT NULL
    AND dolocationid IS NOT NULL
    AND TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, SECOND) BETWEEN 60 AND 86400