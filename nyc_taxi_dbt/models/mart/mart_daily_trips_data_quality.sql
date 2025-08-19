{{ config(materialized='table') }}

SELECT
  -- Date
  DATE_TRUNC(pickup_datetime, DAY) AS trip_date,
  
  -- Comptes par indicateur de qualité
  COUNT(*) AS total_trips,
  SUM(CASE WHEN is_valid = 1 THEN 1 ELSE 0 END) AS valid_trips,
  SUM(CASE WHEN is_valid = 0 THEN 1 ELSE 0 END) AS invalid_trips,
  
  -- Détails des erreurs
  SUM(CASE WHEN data_quality_check != 'Valid' THEN 1 ELSE 0 END) AS data_quality_issues,
  SUM(CASE WHEN missing_data IS NOT NULL THEN 1 ELSE 0 END) AS missing_data_issues,
  
  -- Métriques financières
  SUM(CASE WHEN is_valid = 1 THEN fare_amount ELSE 0 END) AS total_valid_fare,
  SUM(CASE WHEN is_valid = 1 THEN total_amount ELSE 0 END) AS total_valid_revenue,
  AVG(CASE WHEN is_valid = 1 AND trip_distance > 0 
      THEN fare_amount / trip_distance 
      ELSE NULL 
  END) AS avg_fare_per_mile_valid,
  
  -- Taux de qualité
  ROUND((SUM(CASE WHEN is_valid = 1 THEN 1 ELSE 0 END) * 100.0) / 
        NULLIF(COUNT(*), 0), 2) AS data_quality_score
  
FROM {{ ref('int_yellow_trips_validated') }}
GROUP BY 1
ORDER BY 1