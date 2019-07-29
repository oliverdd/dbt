with orders as (
    select 
        day::date as day,
        sku,
        asin,
        sum(cost) as cost,
        sum(attributedSales7d) as attributedSales7d,
        sum(attributedunitsordered7d) as attributedUnitsOrdered7d
    from {{ ref('stg_amazon_sponsored_products') }}
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
        cost/nullif(attributedSales7d,0) as "AcOS %",
        attributedSales7d/nullif(cost,0) as "RoAS %",
        cost-attributedSales7d as "CM %",
        sum(attributedSales7d) over (partition by sku order by day rows between 2 preceding and current row) as y3,
        sum(attributedSales7d) over (partition by sku order by day rows between 6 preceding and current row) as y7,
        sum(attributedSales7d) over (partition by sku order by day rows between 29 preceding and current row) as y30
    from orders
    order by 2 asc, 1 asc
),

final as (
    select
        sku,
        asin,
        null as product,
        null as price,
        round("AcOS %"*100.0,2) as "AcOS %",
        round("RoAS %"*100.0,2) as "RoAS %",
        round("CM %",2) as "CM %",
        attributedSales7d as "Y Sales",
        round(y3/3,2) as "Y3 Sales",
        round(y7/7,2) as "Y7 Sales",
        round(y30/30,2) as "Y30 Sales"
    from metrics m
    join recent r on m.day = r.day
    order by y30 desc nulls last
)

select * from final