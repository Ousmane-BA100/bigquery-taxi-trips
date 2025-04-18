{{ config(materialized='table', partition_by={ "field": "report_date", "data_type": "date" }) }}
SELECT 
    pickup_date as report_date,
    COUNT(*) as total_records,
    -- Qualité globale
    SUM(CASE WHEN data_quality_check = 'Valid' THEN 1 ELSE 0 END) as valid_records,
    SUM(CASE WHEN data_quality_check != 'Valid' THEN 1 ELSE 0 END) as invalid_records,
    -- Détail des problèmes de qualité
    SUM(CASE WHEN data_quality_check = 'Invalid trip duration' THEN 1 ELSE 0 END) as invalid_duration,
    SUM(CASE WHEN data_quality_check = 'Zero distance with duration' THEN 1 ELSE 0 END) as zero_distance,
    SUM(CASE WHEN data_quality_check = 'Negative fare' THEN 1 ELSE 0 END) as negative_fare,
    SUM(CASE WHEN data_quality_check = 'Invalid passenger count' THEN 1 ELSE 0 END) as invalid_passengers,
    -- Données incomplètes
    SUM(CASE WHEN missing_values_flag != 'Complete' THEN 1 ELSE 0 END) as incomplete_records,
    -- Taux de qualité (%)
    ROUND(100.0 * SUM(CASE WHEN data_quality_check = 'Valid' THEN 1 ELSE 0 END) / COUNT(*), 2) as valid_rate,
    ROUND(100.0 * SUM(CASE WHEN missing_values_flag = 'Complete' THEN 1 ELSE 0 END) / COUNT(*), 2) as complete_rate
FROM {{ ref('int_yellow_trips_validated') }}
GROUP BY 1