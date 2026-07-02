

SELECT
   
    COALESCE(
        processor_brand, 
        'Unknown'
        )
 as processor_brand,
   
    COALESCE(
        num_cores, 
        '-1'
        )
 as num_cores,
   
    COALESCE(
        processor_speed, 
        '-1'
        )
 as processor_speed,
   
    COALESCE(
        battery_capacity, 
        '-1'
        )
 as battery_capacity,
   COUNT(*) as combination_count 
FROM "dbt_course"."smartphones"."stg_smartphones"
GROUP BY processor_brand,
         num_cores,
         processor_speed,
         battery_capacity