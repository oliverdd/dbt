with source as (
    select
    	day::date as day,
        sku,
        asin,
        cost as cost,
        attributedSales7d,
        attributedUnitsOrdered7d
    from raw.perfect_keto_amazon_advertising.sponsored_products_report_product_ads
    left join raw.perfect_keto_amazon_advertising.product_ads using (adid)
)

select * from source