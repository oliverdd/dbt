with skus as (
    select
        coalesce(m.sku,s.sku) as sku,
        coalesce(m.asin,s.asin) as asin
    from {{ ref('stg_amazon_mws') }} m
    full outer join {{ ref('stg_amazon_sponsored_products') }} s on m.sku = s.sku
),

amazon_main_metrics as (
    select 
        distinct s.sku,
        s.asin,
        round(zeroifnull(m.price),2) as "Price",
        sp."AcOS %" as "AcOS %",
        sp."RoAS %" as "RoAS %",
        sp."CM %" as "CM %",
        round(zeroifnull(m."Y Sales"+sp."Y Sales"),2) as "Y Sales",
        round(zeroifnull(m."Y3 Sales"+sp."Y3 Sales"),2) as "Y3 Sales",
        round(zeroifnull(m."Y7 Sales"+sp."Y7 Sales"),2) as "Y7 Sales",
        round(zeroifnull(m."Y30 Sales"+sp."Y30 Sales"),2) as "Y30 Sales"
    from skus s
    full join {{ ref('fct_amazon_mws_metrics') }} m on s.sku = m.sku
    full join {{ ref('fct_amazon_sp_metrics') }} sp on s.sku = sp.sku
    order by round(zeroifnull(m."Y30 Sales"+sp."Y30 Sales"),2) desc nulls last
)

select * from amazon_main_metrics