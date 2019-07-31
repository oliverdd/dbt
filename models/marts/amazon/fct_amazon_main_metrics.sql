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
        sp."RoAS $" as "RoAS $",
        round((sp."CM Total"+m."CM Total")/(sp."Y Sales"+m."Y Sales")*100.0,2) as "CM %",
        round(zeroifnull(m."Y Units"+sp."Y Units"),2) as "Y Units",
        round(zeroifnull(m."Y3 Units"+sp."Y3 Units"),2) as "Y3 Units",
        round(zeroifnull(m."Y7 Units"+sp."Y7 Units"),2) as "Y7 Units",
        round(zeroifnull(m."Y30 Units"+sp."Y30 Units"),2) as "Y30 Units"
    from skus s
    full join {{ ref('fct_amazon_mws_metrics') }} m on s.sku = m.sku
    full join {{ ref('fct_amazon_sp_metrics') }} sp on s.sku = sp.sku
    order by round(zeroifnull(m."Y30 Units"+sp."Y30 Units"),2) desc nulls last
)

select * from amazon_main_metrics