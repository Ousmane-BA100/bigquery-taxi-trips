{{ config(materialized='table', 
  partition_by={
    "field": "trip_date",
    "data_type": "date"
  })
}}

SELECT
  pickup_date as trip_date,
  COUNT(*) as total_trips,
  COUNT(DISTINCT vendor_id) as active_vendors,
  
  -- Métriques de passagers
  SUM(passenger_count) as total_passengers,
  AVG(passenger_count) as avg_passengers_per_trip,
  
  -- Métriques de distance et durée
  AVG(trip_distance) as avg_distance,
  AVG(trip_duration_seconds) / 60 as avg_duration_minutes,
  
  -- Métriques de revenus
  SUM(fare_amount) as total_fare,
  SUM(tip_amount) as total_tips,
  SUM(total_amount) as total_revenue,
  
  -- Calcul du pourcentage de pourboire
  AVG(tip_amount / NULLIF(fare_amount, 0)) * 100 as avg_tip_percentage,
  
  -- Qualité des données
  COUNT(CASE WHEN data_quality_check != 'Valid' THEN 1 END) as invalid_trips,
  COUNT(CASE WHEN missing_values_flag != 'Complete' THEN 1 END) as incomplete_records
  
FROM {{ ref('int_yellow_trips_validated') }}
WHERE data_quality_check = 'Valid'
GROUP BY 1

