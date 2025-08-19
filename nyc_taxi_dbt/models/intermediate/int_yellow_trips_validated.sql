{{ config(materialized='table') }}

WITH trip_validations AS (
  SELECT
    -- Identifiants et horodatages
    t.*,
    
    -- Informations de localisation
    CAST(zsm.station_id AS STRING) AS station_id,
    zsm.station_name,
    zsm.station_zone,
    
    -- Validations de données
    CASE
      WHEN t.trip_distance <= 0 OR t.fare_amount <= 0 THEN 'Invalid distance or fare'
      WHEN t.passenger_count < 1 THEN 'Invalid passenger count'
      WHEN t.trip_duration_seconds < 60 THEN 'Trip too short'
      WHEN t.trip_duration_seconds > 86400 THEN 'Trip too long'
      WHEN t.pickup_datetime > t.dropoff_datetime THEN 'Invalid time range'
      ELSE 'Valid'
    END AS data_quality_check,
    
    -- Indicateurs de valeurs manquantes
    CASE
      WHEN t.trip_distance IS NULL THEN 'Missing trip distance'
      WHEN t.fare_amount IS NULL THEN 'Missing fare amount'
      ELSE NULL
    END AS missing_data,
    
    -- Catégorisation des trajets
    CASE
      WHEN t.trip_distance < 1 THEN 'Very short (<1 mile)'
      WHEN t.trip_distance < 3 THEN 'Short (1-3 miles)'
      WHEN t.trip_distance < 10 THEN 'Medium (3-10 miles)'
      WHEN t.trip_distance < 30 THEN 'Long (10-30 miles)'
      ELSE 'Very long (30+ miles)'
    END AS distance_category,
    
    -- Catégorisation des durées
    CASE
      WHEN t.trip_duration_seconds < 300 THEN 'Very short (<5min)'
      WHEN t.trip_duration_seconds < 900 THEN 'Short (5-15min)'
      WHEN t.trip_duration_seconds < 1800 THEN 'Medium (15-30min)'
      WHEN t.trip_duration_seconds < 3600 THEN 'Long (30-60min)'
      ELSE 'Very long (60+ min)'
    END AS duration_category,
    
    -- Indicateur de validité du trajet
    CASE
      WHEN t.trip_distance <= 0 OR t.fare_amount <= 0 THEN 0
      WHEN t.passenger_count < 1 THEN 0
      WHEN t.trip_duration_seconds < 60 THEN 0
      WHEN t.trip_duration_seconds > 86400 THEN 0
      WHEN t.pickup_datetime > t.dropoff_datetime THEN 0
      WHEN t.trip_distance IS NULL OR t.fare_amount IS NULL THEN 0
      ELSE 1
    END AS is_valid
    
  FROM {{ ref('stg_yellow_trips') }} t
  LEFT JOIN {{ ref('stg_zone_to_station_mapping') }} zsm
    ON t.pickup_location_id = zsm.taxi_zone_id
)

SELECT
  *,
  -- Date de la course
  EXTRACT(DATE FROM pickup_datetime) AS trip_date,
  
  -- Heure de la journée
  EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
  
  -- Jour de la semaine (1=Dim, 7=Sam)
  EXTRACT(DAYOFWEEK FROM pickup_datetime) AS day_of_week
  
FROM trip_validations