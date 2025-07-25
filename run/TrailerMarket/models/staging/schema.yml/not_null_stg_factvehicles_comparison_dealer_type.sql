select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select dealer_type
from "postgres"."public"."stg_factvehicles_comparison"
where dealer_type is null



      
    ) dbt_internal_test