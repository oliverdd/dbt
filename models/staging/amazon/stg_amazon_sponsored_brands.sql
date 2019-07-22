with source as (
    select 
        day::date as day,
        campaignname,
        cost,
        attributedSales14d
    from raw.perfect_keto_amazon_advertising.sponsored_brands_report_campaigns
)

select * from source 