select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select mileage_km
from "postgres"."public"."stg_factvehicles_comparison"
where mileage_km is null



      
    ) dbt_internal_test