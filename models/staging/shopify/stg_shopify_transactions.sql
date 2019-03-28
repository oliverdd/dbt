with transactions_base as (
    select * from raw.perfect_keto_shopify.transactions
),

renamed_transactions as (
    select
        admin_graphql_api_id,
        amount,
        authorization,
        to_timestamp(created_at, 'yyyy-mm-ddThh24:mi:ss-TZH:TZM') as created_at,
        currency,
        error_code,
        gateway,
        id,
        kind,
        message,
        order_id,
        parent_id,
        payment_details,
        receipt,
        source_name,
        status,
        test,
        _sdc_batched_at,
        _sdc_extracted_at,
        _sdc_received_at,
        _sdc_sequence,
        _sdc_table_version,
        location_id,
        device_id,
        user_id
    from transactions_base
)

select * from renamed_transactions