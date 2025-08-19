{{ config(materialized='table') }}

WITH daily_valid_metrics AS (
  SELECT
    -- Dimensions
    DATE_TRUNC(t.pickup_datetime, DAY) AS trip_date,
    t.distance_category,
    t.duration_category,
    
    -- Comptes et sommes
    COUNT(*) AS total_trips,
    SUM(t.passenger_count) AS total_passengers,
    SUM(t.fare_amount) AS total_fare_amount,
    SUM(t.tip_amount) AS total_tips,
    SUM(t.total_amount) AS total_revenue,
    AVG(t.fare_amount) AS avg_fare,
    AVG(t.tip_amount) AS avg_tip,
    AVG(t.tip_amount / NULLIF(t.fare_amount, 0)) AS avg_tip_percentage,
    AVG(t.trip_duration_seconds) / 60 AS avg_trip_duration_minutes,
    
    -- Indicateurs de performance
    AVG(CASE 
      WHEN t.trip_distance > 0 THEN t.fare_amount / t.trip_distance 
      ELSE NULL 
    END) AS avg_fare_per_mile,
    
    AVG(t.trip_distance / NULLIF(t.trip_duration_seconds / 3600.0, 0)) AS avg_speed_mph
    
  FROM {{ ref('int_yellow_trips_validated') }} t
  WHERE t.is_valid = 1
  GROUP BY 1, 2, 3
)

SELECT
  *,
  -- Pourcentages
  ROUND((total_trips * 100.0) / SUM(total_trips) OVER (PARTITION BY trip_date), 2) AS pct_of_daily_trips,
  
  -- Tendance sur 7 jours
  AVG(total_trips) OVER (
    PARTITION BY distance_category, duration_category 
    ORDER BY trip_date 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS avg_7day_trips
  
FROM daily_valid_metrics
ORDER BY 
  trip_date DESC,
  total_trips DESC