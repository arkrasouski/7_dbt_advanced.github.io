{{
  config(
     materialized = 'table',
     schema = 'smartphones',
     alias='mart_proc_accum_top10'
   )
}}

SELECT
    processor_brand,
    num_cores,
    processor_speed,
    battery_capacity
FROM {{ ref('int__smartphones_combination') }}
ORDER BY combination_count ASC
LIMIT 10

