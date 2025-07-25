
  create view "postgres"."public"."stg_factvehicles_comparison__dbt_tmp"
    
    
  as (
    select
    vehicle_id,
    mark_id,
    model_id,
    category_id,
    body_id,
    dealer_id,
    year,
    price_eur,
    price_usd,
    price_uah,
    coalesce(mileage_km, -1) as mileage_km,  -- замінили null на -1
    currency_code,
    added_at,
    expired_at,
    country_code
from "postgres"."public"."factvehicles_comparison"
  );