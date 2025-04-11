{{ config(materialized='table') }}

SELECT 
  *,
  -- Gestion des valeurs nulles dans les colonnes clés
  COALESCE(trip_duration_seconds, 0) AS trip_duration_seconds, -- Remplacer NULL par 0
  COALESCE(trip_distance, 0) AS trip_distance, -- Remplacer NULL par 0
  COALESCE(fare_amount, 0) AS fare_amount, -- Remplacer NULL par 0
  COALESCE(passenger_count, 1) AS passenger_count, -- Remplacer NULL par 1 (au moins un passager)
  -- Indicateurs de qualité des données
  CASE 
    WHEN trip_duration_seconds < 0 THEN 'Invalid trip duration'
    WHEN trip_distance = 0 AND trip_duration_seconds > 0 THEN 'Zero distance with duration'
    WHEN fare_amount < 0 THEN 'Negative fare'
    WHEN passenger_count < 1 THEN 'Invalid passenger count'
    ELSE 'Valid' 
  END as data_quality_check,
  
  -- Indiquer si la ligne contient des valeurs manquantes
  CASE
    WHEN pickup_datetime IS NULL OR dropoff_datetime IS NULL THEN 'Missing timestamp'
    WHEN trip_distance IS NULL THEN 'Missing trip distance'
    WHEN fare_amount IS NULL THEN 'Missing fare amount'
    WHEN passenger_count IS NULL THEN 'Missing passenger count'
    ELSE 'Complete'
  END as missing_values_flag
  
FROM {{ ref('stg_yellow_trips') }}
WHERE pickup_datetime IS NOT NULL 
AND dropoff_datetime IS NOT NULL;
