with source as (
    
    select * from raw.perfect_keto_shopify.products
    
),

renamed as (
    
    select 
    
        id as product_id,
        product_type,
        title as product_title,
        body_html,
        handle,
        published_scope,
        tags,
        template_suffix,
        vendor,
        
        -- nested
        image,
        images,
        options,
        variants,
        
        -- dates
        created_at,
        published_at,
        updated_at
        
    from source

)

select * from renamed