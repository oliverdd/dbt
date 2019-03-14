with ad_source as (
    select * from raw.perfect_keto_facebook_ads.ads
    where creative is not null
)

select * from ad_source
