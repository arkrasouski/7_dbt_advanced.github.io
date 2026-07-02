
SELECT
    processor_brand,
    num_cores,
    processor_speed
FROM "dbt_course"."smartphones"."int_smartphones_combination"
GROUP BY 
    processor_brand,
    num_cores,
    processor_speed
ORDER BY SUM(combination_count) DESC
LIMIT 10