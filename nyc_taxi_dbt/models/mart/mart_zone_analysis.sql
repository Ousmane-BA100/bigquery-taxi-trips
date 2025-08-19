{{ config(materialized='table') }}

WITH zone_metrics AS (
  SELECT
    -- Dimensions
    t.pickup_location_id,
    DATE_TRUNC(t.pickup_datetime, DAY) AS trip_date,
    
    -- Comptes et sommes
    COUNT(*) AS total_trips,
    SUM(t.passenger_count) AS total_passengers,
    SUM(t.fare_amount) AS total_fare_amount,
    SUM(t.tip_amount) AS total_tips,
    SUM(t.total_amount) AS total_revenue,
    AVG(t.fare_amount) AS avg_fare,
    AVG(t.tip_amount) AS avg_tip,
    AVG(t.tip_amount / NULLIF(t.fare_amount, 0)) AS avg_tip_percentage,
    AVG(t.trip_distance) AS avg_trip_distance,
    AVG(t.trip_duration_seconds) / 60 AS avg_trip_duration_minutes,
    
    -- Comptes conditionnels
    SUM(CASE WHEN t.time_of_day = 'Morning' THEN 1 ELSE 0 END) AS morning_trips,
    SUM(CASE WHEN t.time_of_day = 'Afternoon' THEN 1 ELSE 0 END) AS afternoon_trips,
    SUM(CASE WHEN t.time_of_day = 'Evening' THEN 1 ELSE 0 END) AS evening_trips,
    SUM(CASE WHEN t.time_of_day = 'Night' THEN 1 ELSE 0 END) AS night_trips
    
  FROM {{ ref('fct_taxi_trips_with_weather') }} t
  WHERE t.is_valid = 1
  GROUP BY 1, 2
)

SELECT
  z.zone_name AS zone,
  z.borough,
  m.*,
  
  -- Densité de trajets (commentée car nécessite la colonne geography)
  -- m.total_trips / NULLIF(ST_AREA(z.geography) / 1000000, 0) AS trips_per_sq_km,
  NULL AS trips_per_sq_km,  -- Valeur par défaut en attendant d'avoir les données géographiques
  
  -- Répartition temporelle
  ROUND((m.morning_trips * 100.0) / NULLIF(m.total_trips, 0), 2) AS pct_morning_trips,
  ROUND((m.afternoon_trips * 100.0) / NULLIF(m.total_trips, 0), 2) AS pct_afternoon_trips,
  ROUND((m.evening_trips * 100.0) / NULLIF(m.total_trips, 0), 2) AS pct_evening_trips,
  ROUND((m.night_trips * 100.0) / NULLIF(m.total_trips, 0), 2) AS pct_night_trips,
  
  -- Revenu par passager
  m.total_revenue / NULLIF(m.total_passengers, 0) AS revenue_per_passenger
  
FROM zone_metrics m
JOIN {{ ref('stg_zones') }} z 
  ON m.pickup_location_id = z.location_id
ORDER BY 
  m.trip_date,
  m.total_trips DESC