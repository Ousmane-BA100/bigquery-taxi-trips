{{ config(materialized='view') }}

SELECT
  -- Identification des courses
  CAST(NULLIF(VendorID, '') AS STRING) as vendor_id,
  
  -- Horodatage
  tpep_pickup_datetime as pickup_datetime,
  tpep_dropoff_datetime as dropoff_datetime,
  
  -- Détails des courses
  CAST(NULLIF(passenger_count, '') AS INT64) as passenger_count,
  CAST(NULLIF(trip_distance, '') AS FLOAT64) as trip_distance,
  CAST(NULLIF(RatecodeID, '') AS INT64) as ratecode_id,
  store_and_fwd_flag,
  CAST(NULLIF(PULocationID, '') AS INT64) as pickup_location_id,
  CAST(NULLIF(DOLocationID, '') AS INT64) as dropoff_location_id,
  CAST(NULLIF(payment_type, '') AS INT64) as payment_type_id,
  
  -- Coûts
  CAST(NULLIF(fare_amount, '') AS FLOAT64) as fare_amount,
  CAST(NULLIF(extra, '') AS FLOAT64) as extra,
  CAST(NULLIF(mta_tax, '') AS FLOAT64) as mta_tax,
  CAST(NULLIF(tip_amount, '') AS FLOAT64) as tip_amount,
  CAST(NULLIF(tolls_amount, '') AS FLOAT64) as tolls_amount,
  CAST(NULLIF(improvement_surcharge, '') AS FLOAT64) as improvement_surcharge,
  CAST(NULLIF(total_amount, '') AS FLOAT64) as total_amount,
  CAST(NULLIF(congestion_surcharge, '') AS FLOAT64) as congestion_surcharge,
  
  -- Métadonnées
  EXTRACT(DATE FROM tpep_pickup_datetime) as pickup_date,
  EXTRACT(YEAR FROM tpep_pickup_datetime) as pickup_year,
  EXTRACT(MONTH FROM tpep_pickup_datetime) as pickup_month,
  EXTRACT(DAY FROM tpep_pickup_datetime) as pickup_day,
  EXTRACT(HOUR FROM tpep_pickup_datetime) as pickup_hour,
  EXTRACT(DAYOFWEEK FROM tpep_pickup_datetime) as pickup_weekday,
  
  -- Durée de la course
  TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, SECOND) as trip_duration_seconds

FROM {{ source('nyc_taxi_data', 'raw_yellow_trips') }}
