with raw as (
    select
        vehicle_id,
        mark_id,
        model_id,
        category_id,
        body_id,
        city_id,
        state_id,
        -- dealer_id переводимо в dealer_type
        case
            when dealer_id = 0 then 'unknown'
            when dealer_id = 1 then 'private'
            when dealer_id = 2 then 'company'
            else 'unknown'
        end as dealer_type,
        year,
        price_eur,
        price_usd,
        price_uah,
        -- mileage_km: null заміняємо на -1
        coalesce(mileage_km, -1) as mileage_km,
        currency_code,
        -- залишаємо null у датах added_at, expired_at
        added_at,
        expired_at,
        country_code
    from "postgres"."public"."factvehicles_new"
)

select * from raw