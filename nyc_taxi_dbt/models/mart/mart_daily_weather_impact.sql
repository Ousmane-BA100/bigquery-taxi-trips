{{ config(materialized='table') }}

SELECT
  pickup_date,
  weather_condition,
  
  -- Métriques de volume
  COUNT(*) AS trip_count,
  COUNT(DISTINCT pickup_location_id) AS active_pickup_zones,
  COUNT(DISTINCT dropoff_location_id) AS active_dropoff_zones,
  
  -- Métriques de prix et distance
  AVG(fare_amount) AS avg_fare,
  AVG(total_amount) AS avg_total_amount,
  AVG(tip_amount) AS avg_tip,
  AVG(tip_amount / NULLIF(fare_amount, 0)) * 100 AS avg_tip_percentage,
  AVG(trip_distance) AS avg_distance,
  AVG(trip_duration_seconds) / 60 AS avg_duration_minutes,
  
  -- Métriques météo
  AVG(avg_temperature_f) AS avg_temperature,
  MAX(precipitation_inches) AS max_precipitation,
  MIN(min_visibility_miles) AS min_visibility
  
FROM {{ ref('fct_taxi_trips_with_weather') }}
GROUP BY 
  pickup_date,
  weather_condition
ORDER BY 
  pickup_date DESC,
  weather_condition