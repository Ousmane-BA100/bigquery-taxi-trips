{{ config(
  materialized='table',
  partition_by={
    "field": "pickup_date",
    "data_type": "date"
  }
) }}

WITH trips_with_weather AS (
  SELECT
    -- Identifiants et horodatages
    t.pickup_datetime,
    t.dropoff_datetime,
    TIMESTAMP_TRUNC(t.pickup_datetime, HOUR) AS pickup_hour,
    DATE(t.pickup_datetime) AS pickup_date,
    
    -- Détails du trajet
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.tip_amount,
    t.total_amount,
    t.trip_duration_seconds,
    
    -- Informations de localisation
    t.pickup_location_id,
    t.dropoff_location_id,
    t.station_id,
    
    -- Données météo
    w.avg_temperature_f,
    w.avg_temperature_c,
    w.total_precipitation,
    w.min_visibility_miles,
    w.avg_wind_speed_knots,
    w.max_wind_gust_knots,
    w.dominant_weather_condition,
    w.precipitation_intensity,
    w.visibility_condition,
    w.wind_condition,
    
    -- Calculs dérivés
    t.fare_amount / NULLIF(t.trip_distance, 0) AS fare_per_mile,
    t.trip_distance / NULLIF(t.trip_duration_seconds / 3600.0, 0) AS avg_speed_mph,
    
    -- Catégories temporelles
    CASE
      WHEN EXTRACT(HOUR FROM t.pickup_datetime) BETWEEN 5 AND 11 THEN 'Morning'
      WHEN EXTRACT(HOUR FROM t.pickup_datetime) BETWEEN 12 AND 16 THEN 'Afternoon'
      WHEN EXTRACT(HOUR FROM t.pickup_datetime) BETWEEN 17 AND 21 THEN 'Evening'
      ELSE 'Night'
    END AS time_of_day,
    
    -- Indicateurs d'impact météo
    CASE
      WHEN w.dominant_weather_condition IN ('Rain', 'Snow') THEN 1
      ELSE 0
    END AS is_precipitation,
    
    CASE
      WHEN w.min_visibility_miles < 1 THEN 1
      ELSE 0
    END AS low_visibility,
    
    -- Indicateur de qualité
    w.is_complete_data AS has_complete_weather_data,
    
    -- Propagation de l'indicateur de validité
    t.is_valid
    
  FROM {{ ref('int_yellow_trips_validated') }} t
  LEFT JOIN {{ ref('int_weather_hourly') }} w
    ON CAST(TIMESTAMP_TRUNC(t.pickup_datetime, HOUR) AS TIMESTAMP) = CAST(w.observation_hour AS TIMESTAMP)
    AND CAST(t.station_id AS STRING) = CAST(w.station AS STRING)
  WHERE t.is_valid = 1
)

SELECT
  *,
  -- Calcul du pourcentage de pourboire
  ROUND((tip_amount / NULLIF(fare_amount, 0)) * 100, 2) AS tip_percentage,
  
  -- Indicateur de trajet long
  CASE
    WHEN trip_duration_seconds > 1800 THEN 1  -- plus de 30 minutes
    ELSE 0
  END AS is_long_trip,
  
  -- Indicateur de trajet cher
  CASE
    WHEN fare_amount / NULLIF(trip_distance, 0) > 10 THEN 1  -- plus de 10$/mile
    ELSE 0
  END AS is_expensive_trip
  
FROM trips_with_weather