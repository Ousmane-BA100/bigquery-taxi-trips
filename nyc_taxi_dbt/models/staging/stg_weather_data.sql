{{ config(materialized='table') }}

SELECT
  -- Identifiants
  station,
  
  -- Localisation
  lat,
  lon,
  
  -- Données météorologiques
  temp_f AS temperature_f,
  (temp_f - 32) * 5/9 AS temperature_c,
  precip_in AS precipitation_inches,
  vis_miles AS visibility_miles,
  wind_speed_kt AS wind_speed_knots,
  wind_gust_kt AS wind_gust_knots,
  wind_dir_deg AS wind_direction_degrees,
  sky_cover1 AS sky_condition,
  weather_codes,
  
  -- Horodatage
  valid AS observation_time,
  TIMESTAMP_TRUNC(valid, HOUR) AS observation_hour,
  EXTRACT(DATE FROM valid) AS observation_date,
  EXTRACT(DAYOFWEEK FROM valid) AS day_of_week,
  
  -- Filtrage des valeurs nulles
  CASE WHEN temp_f IS NULL THEN 1 ELSE 0 END AS is_temperature_null,
  CASE WHEN precip_in IS NULL THEN 1 ELSE 0 END AS is_precipitation_null,
  CASE WHEN vis_miles IS NULL THEN 1 ELSE 0 END AS is_visibility_null
  
FROM {{ source('weather_data', 'raw_weather_data') }}
WHERE 
  station IS NOT NULL
  AND valid IS NOT NULL
  AND temp_f IS NOT NULL