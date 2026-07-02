
    
    

select
    brand_name as unique_field,
    count(*) as n_records

from "dbt_course"."smartphones"."int_smartphones_brands"
where brand_name is not null
group by brand_name
having count(*) > 1


