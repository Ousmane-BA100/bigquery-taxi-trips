{{ config(
  materialized='table',
  partition_by={
    "field": "observation_date",
    "data_type": "date"
  }
) }}

SELECT
  station,
  station_name,
  station_zone,
  observation_hour,
  observation_date,
  hour_of_day,
  
  -- Agrégations horaires (si plusieurs mesures par heure)
  AVG(temperature_f) AS avg_temperature_f,
  AVG(temperature_c) AS avg_temperature_c,
  MAX(precipitation_inches) AS precipitation_inches,
  MIN(visibility_miles) AS min_visibility_miles,
  AVG(wind_speed_knots) AS avg_wind_speed_knots,
  MAX(wind_gust_knots) AS max_wind_gust_knots,
  
  -- Indicateurs météo dérivés
  CASE
    WHEN MAX(precipitation_inches) > 0.1 THEN 'Rainy'
    WHEN MIN(visibility_miles) < 3 THEN 'Poor Visibility'
    WHEN AVG(temperature_f) < 32 THEN 'Freezing'
    WHEN AVG(temperature_f) > 85 THEN 'Hot'
    ELSE 'Normal'
  END AS weather_condition,
  
  -- Nombre d'observations utilisées pour cette agrégation
  COUNT(*) AS observation_count
  
FROM {{ ref('stg_weather_data') }}
GROUP BY 
  station,
  station_name,
  station_zone,
  observation_hour,
  observation_date,
  hour_of_day