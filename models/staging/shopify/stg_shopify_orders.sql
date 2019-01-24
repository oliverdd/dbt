with source as (

    select * from raw.perfect_keto_shopify.orders

),

renamed as (
    
    select 
    
        -- ids
        id as order_id,
        app_id,
        source_name as source_name_id,
        name as order_name,
        user_id,
        customer:id as customer_id,
        customer:last_order_id as last_order_id,
        customer:last_order_name::varchar as last_order_name,
        token,
        
        -- customer attributes
        coalesce(
            nullif(lower(email),''), 
            case when note ilike '%email%' 
                then split_part(split_part(lower(note), ': ', 2), 'first name', 1) 
                else null 
            end) as email, --this may not be needed they did some mapping
        contact_email,
        buyer_accepts_marketing,
        confirmed,
        phone as phone_number,
        customer,
        test,
        
        -- order attributes
        location_id,
        number as "number",
        order_number,
        currency,
        presentment_currency,
        financial_status,
        case when financial_status in ('paid', 'partially_paid', 'partially_refunded')
            then 1
            else 0
        end as is_completed,
        fulfillment_status,
        gateway,
        processing_method,
        total_tip_received,
        total_weight,
        total_discounts,
        subtotal_price,
        total_line_items_price,
        total_price,
        total_price_usd,
        total_tax,
        total_shipping_price_set:presentment_money:amount::numeric(38,6) 
            as total_shipping_cost,
        total_shipping_price_set:presentment_money:currency_code::varchar 
            as shipping_currency_code,
        tags,
        taxes_included, --true/false
        order_status_url,
        
        nullif(lower(shipping_address:city::varchar), '') as shipping_city,
        nullif(lower(shipping_address:province::varchar), '') as shipping_province,
        nullif(lower(shipping_address:province_code::varchar), '') as shipping_province_code,
        nullif(lower(shipping_address:country::varchar), '') as shipping_country,
        nullif(lower(shipping_address:country_code::varchar), '') as shipping_country_code,
        nullif(lower(shipping_address:zip::varchar), '') as shipping_zip_code,
        nullif(shipping_address:longitude, '') as shipping_longitude,
        nullif(shipping_address:latitude, '') as shipping_latitude,
        nullif(lower(billing_address:city::varchar), '') as billing_city,
        nullif(lower(billing_address:province::varchar), '') as billing_province,
        nullif(lower(billing_address:province_code::varchar), '') as billing_province_code,
        nullif(lower(billing_address:country::varchar), '') as billing_country,
        nullif(lower(billing_address:country_code::varchar), '') as billing_country_code,
        nullif(lower(billing_address:zip::varchar), '') as billing_zip_code,
        
        -- dates
        created_at,
        processed_at,
        closed_at,
        updated_at,
        cancelled_at,
        cancel_reason,
        
        -- nested
        note,
        total_shipping_price_set,
        
        -- browser attributes
        referring_site,
        browser_ip,
        landing_site,
        landing_site_ref
    
    from source

)

select * from renamed