{{ config(materialized='table') }}

SELECT
  CASE 
    WHEN pickup_location_id IS NOT NULL THEN pickup_location_id
    ELSE dropoff_location_id
  END AS location_id,
  
  COUNT(*) as total_trips,
  
  -- Utilisation de COALESCE pour remplacer les valeurs NULL par des zéros ou une valeur par défaut
  COALESCE(AVG(CASE WHEN pickup_location_id IS NOT NULL THEN fare_amount END), 0) as avg_fare,
  COALESCE(AVG(CASE WHEN pickup_location_id IS NOT NULL THEN trip_distance END), 0) as avg_distance,
  COALESCE(SUM(CASE WHEN pickup_location_id IS NOT NULL THEN total_amount END), 0) as total_revenue
  
FROM {{ ref('int_yellow_trips_validated') }}
WHERE data_quality_check = 'Valid'
GROUP BY location_id
ORDER BY location_id
