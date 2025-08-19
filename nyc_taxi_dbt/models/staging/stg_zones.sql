-- models/staging/stg_zones.sql
{{ config(materialized='view') }}

SELECT
    LocationID as location_id,
    Borough as borough,
    Zone as zone_name,
    service_zone
FROM {{ source('raw_yellow_taxi_trips', 'taxi_zone') }}