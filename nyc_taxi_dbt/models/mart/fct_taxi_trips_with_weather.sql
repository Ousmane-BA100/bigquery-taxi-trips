{{ config(
  materialized='incremental',
  unique_key=['pickup_datetime', 'pickup_location_id', 'dropoff_location_id', 'vendor_id'],
  partition_by={
    "field": "pickup_date",
    "data_type": "date"
  },
  cluster_by=["pickup_location_id", "weather_station"]
) }}

SELECT
  -- Colonnes des trajets
  t.vendor_id,
  t.pickup_datetime,
  t.dropoff_datetime,
  t.passenger_count,
  t.trip_distance,
  t.ratecode_id,
  t.store_and_fwd_flag,
  t.pickup_location_id,
  t.dropoff_location_id,
  t.payment_type_id,
  t.fare_amount,
  t.extra,
  t.mta_tax,
  t.tip_amount,
  t.tolls_amount,
  t.improvement_surcharge,
  t.total_amount,
  t.congestion_surcharge,
  t.pickup_date,
  t.pickup_year,
  t.pickup_month,
  t.pickup_day,
  t.pickup_hour,
  t.pickup_weekday,
  t.trip_duration_seconds,
  t.data_quality_check,
  t.missing_values_flag,
  
  -- Informations de la station météo associée à la zone de pickup
  m.station_id AS weather_station,
  m.station_name,
  m.station_zone,
  
  -- Données météo horaires pour cette station
  w.avg_temperature_f,
  w.avg_temperature_c,
  w.precipitation_inches,
  w.min_visibility_miles,
  w.avg_wind_speed_knots,
  w.max_wind_gust_knots,
  w.weather_condition

FROM {{ ref('int_yellow_trips_validated') }} t

-- Joindre les trajets avec le mapping zone -> station basé sur le lieu de pickup
LEFT JOIN {{ ref('stg_zone_to_station_mapping') }} m
  ON t.pickup_location_id = m.taxi_zone_id

-- Joindre avec les données météo horaires pour la station et l'heure correspondantes
LEFT JOIN {{ ref('int_weather_hourly') }} w
  ON DATETIME_TRUNC(t.pickup_datetime, HOUR) = w.observation_hour
  AND m.station_id = w.station

-- Filtrer sur la qualité des données + condition d'incrément
WHERE t.data_quality_check = 'Valid'
{% if is_incremental() %}
  AND t.pickup_date > (SELECT MAX(pickup_date) FROM {{ this }})
{% endif %}
