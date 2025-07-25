
    
    

select
    vehicle_id as unique_field,
    count(*) as n_records

from "postgres"."public"."stg_factvehicles_comparison"
where vehicle_id is not null
group by vehicle_id
having count(*) > 1


