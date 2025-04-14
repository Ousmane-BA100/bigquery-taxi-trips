{{ config(materialized='table', partition_by={
  "field": "trip_date",
  "data_type": "date"
}) }}

SELECT
  pickup_date as trip_date,
  COUNT(*) as total_trips,
  COUNT(DISTINCT vendor_id) as active_vendors,
  
  -- Gestion des valeurs nulles pour les passagers
  SUM(COALESCE(passenger_count, 0)) as total_passengers,  -- Remplacer NULL par 0
  
  -- Gestion des valeurs nulles pour la distance
  AVG(COALESCE(trip_distance, 0)) as avg_distance,  -- Remplacer NULL par 0
  
  -- Gestion des valeurs nulles pour la durée
  AVG(COALESCE(trip_duration_seconds, 0)) / 60 as avg_duration_minutes,  -- Remplacer NULL par 0
  
  -- Gestion des valeurs nulles pour le montant
  SUM(COALESCE(fare_amount, 0)) as total_fare,  -- Remplacer NULL par 0
  SUM(COALESCE(tip_amount, 0)) as total_tips,  -- Remplacer NULL par 0
  SUM(COALESCE(total_amount, 0)) as total_revenue,  -- Remplacer NULL par 0
  
  -- Calcul du pourcentage de pourboire en évitant la division par 0
  AVG(COALESCE(tip_amount, 0) / NULLIF(COALESCE(fare_amount, 0), 0)) * 100 as avg_tip_percentage,
  
  -- Nombre de trips invalides
  COUNT(CASE WHEN data_quality_check != 'Valid' THEN 1 END) as invalid_trips
  
FROM {{ ref('int_yellow_trips_validated') }}
GROUP BY 1

