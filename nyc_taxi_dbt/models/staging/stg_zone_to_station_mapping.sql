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
    STRUCT('Queens' AS borough, 'JFK' AS station_id),
    STRUCT('Bronx' AS borough, 'LGA' AS station_id),
    STRUCT('Brooklyn' AS borough, 'JFK' AS station_id),
    STRUCT('Manhattan' AS borough, 'NYC' AS station_id),
    STRUCT('Staten Island' AS borough, 'EWR' AS station_id),
    STRUCT('EWR' AS borough, 'EWR' AS station_id)
  ])
),

-- Jointure des zones de taxi avec les stations météo
zone_station AS (
  SELECT
    z.LocationID AS taxi_zone_id,
    z.Zone AS taxi_zone_name,
    z.Borough AS taxi_borough,
    bsm.station_id,
    ws.station_name,
    ws.station_zone
  FROM {{ source('raw_yellow_taxi_trips', 'taxi_zone') }} z
  LEFT JOIN borough_station_mapping bsm
    ON z.Borough = bsm.borough
  LEFT JOIN weather_stations ws
    ON bsm.station_id = ws.station_id
)

SELECT
  taxi_zone_id,
  taxi_zone_name,
  taxi_borough,
  station_id,
  station_name,
  station_zone
FROM zone_station
ORDER BY taxi_borough, taxi_zone_name