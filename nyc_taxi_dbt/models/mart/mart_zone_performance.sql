{{ config(materialized='table') }}

WITH pickup_metrics AS (
  SELECT
    pickup_location_id AS location_id,
    'Pickup' AS zone_role,
    COUNT(*) AS trip_count,
    AVG(fare_amount) AS avg_fare,
    AVG(trip_distance) AS avg_distance,
    SUM(total_amount) AS total_revenue,
    AVG(trip_duration_seconds) / 60 AS avg_duration_minutes
  FROM {{ ref('int_yellow_trips_validated') }}
  WHERE pickup_location_id IS NOT NULL
  AND data_quality_check = 'Valid'
  GROUP BY pickup_location_id
),

dropoff_metrics AS (
  SELECT
    dropoff_location_id AS location_id,
    'Dropoff' AS zone_role,
    COUNT(*) AS trip_count,
    AVG(fare_amount) AS avg_fare,
    AVG(trip_distance) AS avg_distance,
    SUM(total_amount) AS total_revenue,
    AVG(trip_duration_seconds) / 60 AS avg_duration_minutes
  FROM {{ ref('int_yellow_trips_validated') }}
  WHERE dropoff_location_id IS NOT NULL
  AND data_quality_check = 'Valid'
  GROUP BY dropoff_location_id
)

SELECT 
  z.LocationID,
  z.Borough,
  z.Zone,
  z.service_zone,
  COALESCE(p.zone_role, d.zone_role) AS primary_zone_role,
  COALESCE(p.trip_count, 0) AS pickup_trips,
  COALESCE(d.trip_count, 0) AS dropoff_trips,
  COALESCE(p.trip_count, 0) + COALESCE(d.trip_count, 0) AS total_trips,
  COALESCE(p.avg_fare, 0) AS avg_pickup_fare,
  COALESCE(d.avg_fare, 0) AS avg_dropoff_fare,
  COALESCE(p.avg_distance, 0) AS avg_pickup_distance,
  COALESCE(d.avg_distance, 0) AS avg_dropoff_distance,
  COALESCE(p.total_revenue, 0) AS pickup_revenue,
  COALESCE(d.total_revenue, 0) AS dropoff_revenue
FROM {{ source('nyc_taxi_data', 'taxi_zone') }} z
LEFT JOIN pickup_metrics p ON z.LocationID = p.location_id
LEFT JOIN dropoff_metrics d ON z.LocationID = d.location_id