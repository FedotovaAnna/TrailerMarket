
  
    

  create  table "postgres"."public"."fact_vehicles_silver__dbt_tmp"
  
  
    as
  
  (
    

with source_1 as (
    select
        vehicle_id,
        mark_id,
        model_id,
        category_id,
        body_id,
        city_id,
        state_id,
        mileage_km,
        added_at,
        expired_at,
        year,
        price_eur,
        price_usd,
        price_uah,
        null::integer as dealer_id,         -- приводим к integer
        country_code,
        currency_code
    from "postgres"."public"."stg_fact_vehicles"
),

source_2 as (
    select
        vehicle_id,
        mark_id,
        model_id,
        category_id,
        body_id,
        null::integer as city_id,           -- приводим к integer
        null::integer as state_id,          -- приводим к integer
        mileage_km,
        added_at,
        expired_at,
        year,
        price_eur,
        price_usd,
        price_uah,
        dealer_id,
        country_code,
        currency_code
    from "postgres"."public"."stg_factvehicles_comparison"
)

select * from source_1
union all
select * from source_2
  );
  