
    
    

with all_values as (

    select
        num_cores as value_field,
        count(*) as n_records

    from "dbt_course"."smartphones"."stg_smartphones"
    group by num_cores

)

select *
from all_values
where value_field not in (
    '2','4','6','8'
)


