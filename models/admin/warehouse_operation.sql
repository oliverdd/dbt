{{
    config(
        materialized = 'incremental',
        sql_where = 'TRUE'
    )
}}

select
    current_timestamp() as run_at,
    {{var('resume_warehouse', false)}} as warehouse_resumed,
    {{var('suspend_warehouse', false)}} as warehouse_suspended,
    '{{var('warehouse_name')}}' as warehouse_name