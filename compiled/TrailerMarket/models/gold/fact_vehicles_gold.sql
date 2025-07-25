

select
    f.vehicle_id,
    f.mileage_km,
    f.added_at,
    f.expired_at,
    f.year,
    f.price_eur,
    f.price_usd,
    f.price_uah,
    f.dealer_id,

    c.name as country_name,
    cur."currencyName" as currency_name,
    b.name as body_style_name,
    cat.category_name,
    city.name as city_name,
    mark.name as mark_name,
    model.name as model_name,
    state.name as state_name

from "postgres"."public"."fact_vehicles_silver" f

left join "postgres"."public"."DimCountries" c
    on f.country_code::int = c.value

left join "postgres"."public"."DimCurrency" cur
    on f.currency_code = cur."currencyCode"

left join "postgres"."public"."dim_body_styles" b
    on f.body_id = b.bodystyle_id

left join "postgres"."public"."dim_categories" cat
    on f.category_id = cat.category_value

left join "postgres"."public"."dim_cities" city
    on f.city_id = city.value

left join "postgres"."public"."dim_marks" mark
    on f.mark_id = mark.value

left join "postgres"."public"."dim_models" model
    on f.model_id = model.value

left join "postgres"."public"."dim_states" state
    on f.state_id = state.value