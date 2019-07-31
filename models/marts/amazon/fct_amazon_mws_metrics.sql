with mws_orders as (
    select
        day,
        sku,
        asin,
        avg(price) as price,
        sum(quantityordered) as units_ordered,
        sum(sales) as sales,
        sum(cogs) as cogs,
        sum(amazon_referral) as amazon_referral,
        sum(amazon_fba_fee) as amazon_fba_fee,
        sum(contribution_margin) as contribution_margin
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
        contribution_margin/nullif(sales,0) as "CM %",
        contribution_margin as "CM Total",
        avg(price) over (partition by sku order by day rows between 6 preceding and current row) as price7,
        sum(sales) over (partition by sku order by day rows between 2 preceding and current row) as sales_y3,
        sum(sales) over (partition by sku order by day rows between 6 preceding and current row) as sales_y7,
        sum(sales) over (partition by sku order by day rows between 29 preceding and current row) as sales_y30,
        sum(units_ordered) over (partition by sku order by day rows between 2 preceding and current row) as units_y3,
        sum(units_ordered) over (partition by sku order by day rows between 6 preceding and current row) as units_y7,
        sum(units_ordered) over (partition by sku order by day rows between 29 preceding and current row) as units_y30
    from mws_orders
    where day <> '2019-07-15' and day <> '2019-07-16'
    order by 2 asc, 1 asc
),

mws_final as (
    select
        sku,
        asin,
        price7 as price,
        round("CM %"*100.0,2) as "CM %",
        "CM Total",
        sales as "Y Sales",
        round(sales_y3/3,2) as "Y3 Sales",
        round(sales_y7/7,2) as "Y7 Sales",
        round(sales_y30/30,2) as "Y30 Sales",
        units_ordered as "Y Units",
        round(units_y3/3,2) as "Y3 Units",
        round(units_y7/7,2) as "Y7 Units",
        round(units_y30/30,2) as "Y30 Units"
    from mws_metrics m
    join mws_recent r on m.day = r.day
)

select * from mws_final