with orders as (
    select 
        day::date as day,
        sku,
        asin,
        sum(attributedSales7d) as attributedSales7d,
        sum(advertising_cost) as advertising_cost,
        sum(cogs) as cogs,
        sum(amazon_referral) as amazon_referral,
        sum(amazon_fba_fee) as amazon_fba_fee,
        sum(contribution_margin) as contribution_margin,
        sum(attributedunitsordered7d) as attributedUnitsOrdered7d
    from analytics.stg_amazon_sponsored_products
    group by 1,2,3
    order by 1 desc
),

recent as (
    select max(day) as day
    from orders
),

metrics as (
    select 
        *,
        advertising_cost/nullif(attributedSales7d,0) as "AcOS %",
        attributedSales7d/nullif(advertising_cost,0) as "RoAS $",
        contribution_margin/nullif(attributedSales7d,0) as "CM %",
        contribution_margin as "CM Total",
        sum(attributedSales7d) over (partition by sku order by day rows between 2 preceding and current row) as sales_y3,
        sum(attributedSales7d) over (partition by sku order by day rows between 6 preceding and current row) as sales_y7,
        sum(attributedSales7d) over (partition by sku order by day rows between 29 preceding and current row) as sales_y30,
        sum(attributedunitsordered7d) over (partition by sku order by day rows between 2 preceding and current row) as units_y3,
        sum(attributedunitsordered7d) over (partition by sku order by day rows between 6 preceding and current row) as units_y7,
        sum(attributedunitsordered7d) over (partition by sku order by day rows between 29 preceding and current row) as units_y30
    from orders
    --filter out orders from prime day
    where day <> '2019-07-15' and day <> '2019-07-16'
    order by 2 asc, 1 asc
),

final as (
    select
        sku,
        asin,
        null as product,
        null as price,
        round("AcOS %"*100.0,2) as "AcOS %",
        round("RoAS $",2) as "RoAS $",
        round("CM %"*100.0,2) as "CM %",
        "CM Total",
        attributedSales7d as "Y Sales",
        round(sales_y3/3,2) as "Y3 Sales",
        round(sales_y7/7,2) as "Y7 Sales",
        round(sales_y30/30,2) as "Y30 Sales",
        attributedunitsordered7d as "Y Units",
        round(units_y3/3,2) as "Y3 Units",
        round(units_y7/7,2) as "Y7 Units",
        round(units_y30/30,2) as "Y30 Units"
    from metrics m
    join recent r on m.day = r.day
)
select * from final