

SELECT 
    brand_name,
    model,
    price,
    processor_brand,
    num_cores::int,
    processor_speed,
    battery_capacity::int
FROM "dbt_course"."public"."smartphone_cleaned_v5"
WHERE num_cores > 0 --Добавил для проверки юнит теста