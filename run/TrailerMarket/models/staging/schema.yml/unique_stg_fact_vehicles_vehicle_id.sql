select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    vehicle_id as unique_field,
    count(*) as n_records

from "postgres"."public"."stg_fact_vehicles"
where vehicle_id is not null
group by vehicle_id
having count(*) > 1



      
    ) dbt_internal_test