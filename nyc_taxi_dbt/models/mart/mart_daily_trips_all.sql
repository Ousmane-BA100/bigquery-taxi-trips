{{ config(materialized='table', partition_by={ "field": "trip_date", "data_type": "date" }) }}
SELECT 
    pickup_date as trip_date, 
    COUNT(*) as total_trips,
    COUNT(DISTINCT vendor_id) as active_vendors,
    -- Métriques avec gestion des nulls
    SUM(COALESCE(passenger_count, 0)) as total_passengers,
    AVG(COALESCE(trip_distance, 0)) as avg_distance,
    AVG(COALESCE(trip_duration_seconds, 0)) / 60 as avg_duration_minutes,
    SUM(COALESCE(fare_amount, 0)) as total_fare,
    SUM(COALESCE(tip_amount, 0)) as total_tips,
    SUM(COALESCE(total_amount, 0)) as total_revenue,
    AVG(COALESCE(tip_amount, 0) / NULLIF(COALESCE(fare_amount, 0), 0)) * 100 as avg_tip_percentage
FROM {{ ref('int_yellow_trips_validated') }}
GROUP BY 1