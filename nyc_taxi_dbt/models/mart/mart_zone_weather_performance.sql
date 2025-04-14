{{ config(materialized='table') }}

SELECT
  pickup_location_id,
  weather_condition,
  
  -- Agrégations
  COUNT(*) AS total_trips,
  AVG(fare_amount) AS avg_fare,
  AVG(trip_distance) AS avg_distance,
  SUM(total_amount) AS total_revenue,
  
  -- Mesures d'efficacité
  AVG(trip_distance / NULLIF(trip_duration_seconds / 3600, 0)) AS avg_speed_mph,
  AVG(fare_amount / NULLIF(trip_distance, 0)) AS revenue_per_mile,
  
  -- Métriques météo
  AVG(avg_temperature_f) AS avg_temperature,
  AVG(precipitation_inches) AS avg_precipitation,
  AVG(min_visibility_miles) AS avg_visibility
  
FROM {{ ref('fct_taxi_trips_with_weather') }}
WHERE 
  pickup_location_id IS NOT NULL
GROUP BY 
  pickup_location_id,
  weather_condition