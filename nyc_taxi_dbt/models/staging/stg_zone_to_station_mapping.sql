{{ config(materialized='table') }}

WITH weather_stations AS (
  SELECT 
    station AS station_id,
    station_name,
    station_zone
  FROM (
    SELECT
      station,
      station_name,
      station_zone,
      ROW_NUMBER() OVER(PARTITION BY station ORDER BY valid DESC) as rn
    FROM {{ source('weather_data', 'raw_weather_data') }}
    WHERE station IS NOT NULL
  )
  WHERE rn = 1
),

-- Table de mapping borough vers station (une seule station par borough)
borough_station_mapping AS (
  SELECT * FROM UNNEST([
    STRUCT('Queens' AS borough, 'KJFK' AS station_id),
    STRUCT('Bronx' AS borough, 'KLGA' AS station_id),
    STRUCT('Brooklyn' AS borough, 'KJFK' AS station_id),
    STRUCT('Manhattan' AS borough, 'KNYC' AS station_id),
    STRUCT('Staten Island' AS borough, 'KEWR' AS station_id),
    STRUCT('EWR' AS borough, 'KEWR' AS station_id)
  ])
)

SELECT
  tz.LocationID AS taxi_zone_id,
  tz.Zone AS taxi_zone_name,
  tz.Borough AS taxi_borough,
  ws.station_id,
  ws.station_name,
  ws.station_zone
FROM 
  {{ source('nyc_taxi_data', 'taxi_zone') }} tz
  JOIN borough_station_mapping bsm
    ON tz.Borough = bsm.borough
  JOIN weather_stations ws
    ON bsm.station_id = ws.station_id