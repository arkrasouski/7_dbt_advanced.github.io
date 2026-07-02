

SELECT
    processor_brand,
    num_cores,
    processor_speed,
    battery_capacity
FROM "dbt_course"."smartphones"."int_smartphones_combination"
ORDER BY combination_count DESC
LIMIT 10