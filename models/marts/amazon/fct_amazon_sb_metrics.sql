with orders as (
    select 
        day::date as day,
        campaignname,
        sum(cost) as cost,
        sum(attributedSales14d) as attributedSales14d
    from {{ ref('stg_amazon_sponsored_brands') }}
    group by 1,2
    order by 1 desc
),

recent as (
    select max(day) as day
    from orders
),

metrics as (
    select 
        *,
        cost/nullif(attributedSales14d,0) as "AcOS %",
        attributedSales14d/nullif(cost,0) as "RoAS %",
        cost-attributedSales14d as "CM %",
        sum(attributedSales14d) over (partition by campaignname order by day rows between 2 preceding and current row) as y3,
        sum(attributedSales14d) over (partition by campaignname order by day rows between 6 preceding and current row) as y7,
        sum(attributedSales14d) over (partition by campaignname order by day rows between 29 preceding and current row) as y30
    from orders
    order by 2 asc, 1 asc
),

final as (
    select
        campaignname,
        round("AcOS %"*100.0,2) as "AcOS %",
        round("RoAS %"*100.0,2) as "RoAS %",
        "CM %",
        attributedSales14d as "Y Sales",
        round(y3/3,2) as "Y3 Sales",
        round(y7/7,2) as "Y7 Sales",
        round(y30/30,2) as "Y30 Sales"
    from metrics m
    join recent r on m.day = r.day
    order by y30 desc nulls last
)

select * from final