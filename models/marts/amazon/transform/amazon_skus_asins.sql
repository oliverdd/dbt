with joined as (
    select
        coalesce(m.sku,s.sku) as sku,
        coalesce(m.asin,s.asin) as asin
    from {{ ref('stg_amazon_mws_order_items') }} m
    full outer join {{ ref('stg_amazon_sponsored_products') }} s on m.sku = s.sku
)

select 
	distinct sku,
	asin
from joined 