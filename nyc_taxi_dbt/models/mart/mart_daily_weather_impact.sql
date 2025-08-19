{{ config(materialized='table') }}

WITH weather_impact AS (
  SELECT
    -- Dimensions
    DATE_TRUNC(t.pickup_datetime, DAY) AS trip_date,
    t.dominant_weather_condition,
    t.precipitation_intensity,
    t.visibility_condition,
    t.wind_condition,
    
    -- Comptes et sommes
    COUNT(*) AS total_trips,
    SUM(t.fare_amount) AS total_fare_amount,
    SUM(t.tip_amount) AS total_tips,
    SUM(t.total_amount) AS total_revenue,
    
    -- Métriques moyennes
    AVG(t.fare_amount) AS avg_fare,
    AVG(t.tip_amount) AS avg_tip,
    AVG(t.tip_amount / NULLIF(t.fare_amount, 0)) AS avg_tip_percentage,
    AVG(t.avg_speed_mph) AS avg_speed_mph,
    AVG(t.trip_duration_seconds) / 60 AS avg_trip_duration_minutes,
    
    -- Indicateurs d'impact
    AVG(CASE 
      WHEN t.trip_distance > 0 THEN t.fare_amount / t.trip_distance 
      ELSE NULL 
    END) AS avg_fare_per_mile
    
  FROM {{ ref('fct_taxi_trips_with_weather') }} t
  WHERE t.is_valid = 1
  GROUP BY 1, 2, 3, 4, 5
)

SELECT
  *,
  -- Comparaison avec la moyenne globale
  avg_speed_mph - AVG(avg_speed_mph) OVER (PARTITION BY trip_date) AS speed_impact,
  avg_tip_percentage - AVG(avg_tip_percentage) OVER (PARTITION BY trip_date) AS tip_impact,
  avg_trip_duration_minutes - AVG(avg_trip_duration_minutes) OVER (PARTITION BY trip_date) AS duration_impact,
  
  -- Tendance sur 7 jours
  AVG(total_trips) OVER (
    PARTITION BY dominant_weather_condition, precipitation_intensity 
    ORDER BY trip_date 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS avg_7day_trips
  
FROM weather_impact
ORDER BY 
  trip_date DESC,
  total_trips DESC