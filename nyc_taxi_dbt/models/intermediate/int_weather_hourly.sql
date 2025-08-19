{{ config(materialized='table') }}

WITH hourly_weather AS (
  SELECT
    -- Identifiants
    station,
    
    -- Agrégations temporelles
    observation_hour,
    DATE(observation_hour) AS observation_date,
    
    -- Données météo agrégées
    AVG(temperature_f) AS avg_temperature_f,
    AVG(temperature_c) AS avg_temperature_c,
    SUM(precipitation_inches) AS total_precipitation,
    MIN(visibility_miles) AS min_visibility_miles,
    AVG(wind_speed_knots) AS avg_wind_speed_knots,
    MAX(wind_gust_knots) AS max_wind_gust_knots,
    AVG(wind_direction_degrees) AS avg_wind_direction_degrees,
    
    -- Conditions météo dominantes (simplifiées)
    CASE
      WHEN SUM(CASE WHEN weather_codes LIKE '%RA%' THEN 1 ELSE 0 END) > 0 THEN 'Rain'
      WHEN SUM(CASE WHEN weather_codes LIKE '%SN%' THEN 1 ELSE 0 END) > 0 THEN 'Snow'
      WHEN SUM(CASE WHEN weather_codes LIKE '%FG%' THEN 1 ELSE 0 END) > 0 THEN 'Fog'
      WHEN SUM(CASE WHEN weather_codes LIKE '%CLR%' THEN 1 ELSE 0 END) > 0 THEN 'Clear'
      WHEN SUM(CASE WHEN weather_codes LIKE '%OVC%' THEN 1 ELSE 0 END) > 0 THEN 'Overcast'
      WHEN SUM(CASE WHEN weather_codes LIKE '%SCT%' THEN 1 ELSE 0 END) > 0 THEN 'Scattered Clouds'
      WHEN SUM(CASE WHEN weather_codes LIKE '%BKN%' THEN 1 ELSE 0 END) > 0 THEN 'Broken Clouds'
      WHEN SUM(CASE WHEN weather_codes LIKE '%FEW%' THEN 1 ELSE 0 END) > 0 THEN 'Few Clouds'
      ELSE 'Unknown'
    END AS dominant_weather_condition,
    
    -- Indicateurs de qualité
    SUM(is_temperature_null) AS missing_temperature_readings,
    SUM(is_precipitation_null) AS missing_precipitation_readings,
    SUM(is_visibility_null) AS missing_visibility_readings,
    
    -- Nombre total d'observations
    COUNT(*) AS total_observations
    
  FROM {{ ref('stg_weather_data') }}
  WHERE observation_hour IS NOT NULL
  GROUP BY 
    station,
    observation_hour
)

SELECT
  *,
  
  -- Catégorisation de l'intensité des précipitations
  CASE
    WHEN total_precipitation = 0 THEN 'No rain'
    WHEN total_precipitation < 0.1 THEN 'Light rain'
    WHEN total_precipitation < 0.3 THEN 'Moderate rain'
    ELSE 'Heavy rain'
  END AS precipitation_intensity,
  
  -- Catégorisation de la visibilité
  CASE
    WHEN min_visibility_miles < 0.25 THEN 'Very poor'
    WHEN min_visibility_miles < 0.5 THEN 'Poor'
    WHEN min_visibility_miles < 2 THEN 'Moderate'
    WHEN min_visibility_miles < 5 THEN 'Good'
    ELSE 'Excellent'
  END AS visibility_condition,
  
  -- Catégorisation de la force du vent
  CASE
    WHEN avg_wind_speed_knots < 4 THEN 'Light'
    WHEN avg_wind_speed_knots < 11 THEN 'Moderate'
    WHEN avg_wind_speed_knots < 22 THEN 'Strong'
    WHEN avg_wind_speed_knots < 48 THEN 'Gale'
    WHEN avg_wind_speed_knots < 64 THEN 'Storm'
    ELSE 'Hurricane'
  END AS wind_condition,
  
  -- Indicateur de données complètes
  CASE
    WHEN missing_temperature_readings = 0 
         AND missing_precipitation_readings = 0 
         AND missing_visibility_readings = 0 
         AND total_observations > 0
    THEN 1
    ELSE 0
  END AS is_complete_data

FROM hourly_weather