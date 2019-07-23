with mws_orders as (
    select
        day,
        sku,
        asin,
        avg(price) as price,
        sum(units_ordered) as units_ordered
    from {{ ref('stg_amazon_mws') }}
    where orderstatus != 'Canceled'
    group by 1,2,3
),

mws_recent as (
    select max(day) as day
    from mws_orders
),

mws_metrics as (
    select 
        *,
        avg(price) over (partition by sku order by day rows between 6 preceding and current row) as price7,
        sum(units_ordered) over (partition by sku order by day rows between 2 preceding and current row) as y3,
        sum(units_ordered) over (partition by sku order by day rows between 6 preceding and current row) as y7,
        sum(units_ordered) over (partition by sku order by day rows between 29 preceding and current row) as y30
    from mws_orders
    order by 2 asc, 1 asc
),

mws_final as (
    select
        sku,
        asin,
        price7 as price,
        units_ordered as "Y Sales",
        round(y3/3,2) as "Y3 Sales",
        round(y7/7,2) as "Y7 Sales",
        round(y30/30,2) as "Y30 Sales"
    from mws_metrics m
    join mws_recent r on m.day = r.day
    order by y30 desc nulls last
)

select * from mws_final 