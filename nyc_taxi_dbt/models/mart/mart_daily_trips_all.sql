{{ config(materialized='table') }}

WITH daily_metrics AS (
  SELECT
    -- Dimensions
    DATE_TRUNC(t.pickup_datetime, DAY) AS trip_date,
    t.time_of_day,
    t.dominant_weather_condition,
    t.precipitation_intensity,
    t.visibility_condition,
    t.wind_condition,
    
    -- Comptes et sommes
    COUNT(*) AS total_trips,
    SUM(t.passenger_count) AS total_passengers,
    SUM(t.fare_amount) AS total_fare_amount,
    SUM(t.tip_amount) AS total_tips,
    SUM(t.total_amount) AS total_revenue,
    AVG(t.fare_amount) AS avg_fare,
    AVG(t.tip_amount) AS avg_tip,
    AVG(t.tip_percentage) AS avg_tip_percentage,
    AVG(t.avg_speed_mph) AS avg_speed_mph,
    AVG(t.trip_distance) AS avg_trip_distance,
    AVG(t.trip_duration_seconds) / 60 AS avg_trip_duration_minutes,
    
    -- Comptes conditionnels
    SUM(CASE WHEN t.is_long_trip = 1 THEN 1 ELSE 0 END) AS long_trips,
    SUM(CASE WHEN t.is_expensive_trip = 1 THEN 1 ELSE 0 END) AS expensive_trips,
    SUM(CASE WHEN t.is_precipitation = 1 THEN 1 ELSE 0 END) AS trips_during_precipitation,
    SUM(CASE WHEN t.low_visibility = 1 THEN 1 ELSE 0 END) AS trips_in_low_visibility
    
  FROM {{ ref('fct_taxi_trips_with_weather') }} t
  WHERE t.has_complete_weather_data = 1
  GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
  *,
  -- Calcul des pourcentages
  ROUND((long_trips * 100.0) / NULLIF(total_trips, 0), 2) AS pct_long_trips,
  ROUND((expensive_trips * 100.0) / NULLIF(total_trips, 0), 2) AS pct_expensive_trips,
  ROUND((trips_during_precipitation * 100.0) / NULLIF(total_trips, 0), 2) AS pct_trips_in_precipitation,
  ROUND((trips_in_low_visibility * 100.0) / NULLIF(total_trips, 0), 2) AS pct_trips_low_visibility,
  
  -- Indicateurs de performance
  total_revenue / NULLIF(total_trips, 0) AS revenue_per_trip,
  total_passengers / NULLIF(total_trips, 0) AS avg_passengers_per_trip
  
FROM daily_metrics
ORDER BY trip_date, time_of_day