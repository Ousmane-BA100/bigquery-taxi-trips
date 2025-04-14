{{ config(materialized='view') }}

SELECT
  -- Identifiants et horodatage
  station,
  valid AS observation_time,
  station_name,
  station_zone,
  
  -- Localisation
  lat,
  lon,
  
  -- Données météorologiques
  tmpf AS temperature_f,
  (tmpf - 32) * 5/9 AS temperature_c,  -- Conversion en Celsius
  p01i AS precipitation_inches,
  vsby AS visibility_miles,
  sknt AS wind_speed_knots,
  gust AS wind_gust_knots,
  skyc1 AS sky_condition,
  wxcodes AS weather_codes,
  
  -- Indicateurs temporels pour faciliter les joins
  TIMESTAMP_TRUNC(valid, HOUR) AS observation_hour,
  EXTRACT(DATE FROM valid) AS observation_date,
  EXTRACT(HOUR FROM valid) AS hour_of_day,
  EXTRACT(DAYOFWEEK FROM valid) AS day_of_week
  
FROM {{ source('weather_data', 'raw_weather_data') }}
WHERE valid IS NOT NULL