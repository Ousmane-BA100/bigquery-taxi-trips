{{ config(materialized='table') }}

WITH weather_zone_metrics AS (
  SELECT
    -- Dimensions
    t.pickup_location_id,
    t.dominant_weather_condition,
    t.precipitation_intensity,
    t.visibility_condition,
    t.wind_condition,
    DATE_TRUNC(t.pickup_datetime, DAY) AS trip_date,
    
    -- Comptes et sommes
    COUNT(*) AS total_trips,
    SUM(t.fare_amount) AS total_fare_amount,
    SUM(t.tip_amount) AS total_tips,
    SUM(t.total_amount) AS total_revenue,
    AVG(t.fare_amount) AS avg_fare,
    AVG(t.tip_amount / NULLIF(t.fare_amount, 0)) AS avg_tip_percentage,
    AVG(t.avg_speed_mph) AS avg_speed_mph,
    AVG(t.trip_duration_seconds) / 60 AS avg_trip_duration_minutes
    
  FROM {{ ref('fct_taxi_trips_with_weather') }} t
  WHERE t.is_valid = 1
  GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
  z.zone_name AS zone,
  z.borough,
  w.*,
  
  -- Impact météo sur la vitesse
  w.avg_speed_mph - 
    AVG(w.avg_speed_mph) OVER (PARTITION BY w.pickup_location_id) AS speed_impact_vs_avg,
    
  -- Impact météo sur la durée
  w.avg_trip_duration_minutes - 
    AVG(w.avg_trip_duration_minutes) OVER (PARTITION BY w.pickup_location_id) AS duration_impact_vs_avg,
  
  -- Impact météo sur les pourboires
  w.avg_tip_percentage - 
    AVG(w.avg_tip_percentage) OVER (PARTITION BY w.pickup_location_id) AS tip_impact_vs_avg
    
FROM weather_zone_metrics w
JOIN {{ ref('stg_zones') }} z 
  ON w.pickup_location_id = z.location_id
ORDER BY 
  w.trip_date,
  z.borough,
  z.zone_name,
  w.dominant_weather_condition