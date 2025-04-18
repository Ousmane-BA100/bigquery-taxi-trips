{{ config(materialized='table') }}

SELECT
  -- Colonnes originales provenant du modèle staging
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  
  -- Colonnes avec gestion des valeurs nulles
  COALESCE(passenger_count, 1) AS passenger_count,
  COALESCE(trip_distance, 0) AS trip_distance,
  ratecode_id,
  store_and_fwd_flag,
  pickup_location_id,
  dropoff_location_id,
  payment_type_id,
  COALESCE(fare_amount, 0) AS fare_amount,
  COALESCE(extra, 0) AS extra,
  COALESCE(mta_tax, 0) AS mta_tax,
  COALESCE(tip_amount, 0) AS tip_amount,
  COALESCE(tolls_amount, 0) AS tolls_amount,
  COALESCE(improvement_surcharge, 0) AS improvement_surcharge,
  COALESCE(total_amount, 0) AS total_amount,
  COALESCE(congestion_surcharge, 0) AS congestion_surcharge,
  
  -- Colonnes calculées
  COALESCE(trip_duration_seconds, 0) AS trip_duration_seconds,
  
  -- Indicateurs temporels
  pickup_date,
  pickup_year,
  pickup_month,
  pickup_day,
  pickup_hour,
  pickup_weekday,
  pickup_hour_rounded,
  
  -- Indicateurs de qualité des données
  CASE
    WHEN DATE(pickup_datetime) < '2022-01-01' OR DATE(dropoff_datetime) < '2022-01-01' THEN 'Date trop ancienne'
    WHEN DATE(pickup_datetime) > DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH) OR DATE(dropoff_datetime) > DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH) THEN 'Date future ou trop récente'
    WHEN trip_duration_seconds < 0 THEN 'Invalid trip duration'
    WHEN trip_distance = 0 AND trip_duration_seconds > 60 THEN 'Zero distance with duration'
    WHEN fare_amount < 0 THEN 'Negative fare'
    WHEN passenger_count < 1 THEN 'Invalid passenger count'
    ELSE 'Valid'
  END as data_quality_check,
  
  -- Indicateur valeurs manquantes
  CASE
    WHEN pickup_datetime IS NULL OR dropoff_datetime IS NULL THEN 'Missing timestamp'
    WHEN trip_distance IS NULL THEN 'Missing trip distance'
    WHEN fare_amount IS NULL THEN 'Missing fare amount'
    WHEN passenger_count IS NULL THEN 'Missing passenger count'
    ELSE 'Complete'
  END as missing_values_flag,
  
  -- Indicateur de date valide
  CASE
    WHEN DATE(pickup_datetime) < '2022-01-01' OR DATE(pickup_datetime) > DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH) THEN FALSE
    ELSE TRUE
  END as is_valid_pickup_date,
  
  CASE
    WHEN DATE(dropoff_datetime) < '2022-01-01' OR DATE(dropoff_datetime) > DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH) THEN FALSE
    ELSE TRUE
  END as is_valid_dropoff_date
  
FROM {{ ref('stg_yellow_trips') }}