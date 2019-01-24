with source as (
    
    select * from raw.perfect_keto_shopify.customers
    
),

renamed as (
    
    select
    
        -- ids
        id as customer_id,
        last_order_id,
        last_order_name as last_order_name_id,
        
        -- customer attributes
        lower(email) as email,
        verified_email, -- true/false
        lower(first_name) as first_name,
        lower(last_name) as last_name,
        accepts_marketing,
        orders_count,
        currency,
        phone as phone_number,
        state,
        tags,
        tax_exempt,
        total_spent,
        
        -- nested
        addresses,
        default_address,
        note,
        
        -- dates
        created_at as customer_created_at,
        updated_at

    from source

)

select * from renamed