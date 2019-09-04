with source as (

    select
        amazonorderid as id,
        buyeremail,  
        buyername,
        convert_timezone('UTC','America/Los_Angeles',purchasedate::timestamp_ntz) as created_at,
        ordertotal:Amount::float as order_total,
        case when lower(orderstatus) = 'shipped' then 1 else 0 end as is_completed,
        orderstatus,
        isbusinessorder
    from raw.perfect_keto_amazon_mws.orders
    
)

select * from source