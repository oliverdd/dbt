with orders as (
    
    select * from {{ ref('stg_amazon_mws') }}

),

fields as (

    select 
        
        *,
        
        extract(year from created_at) as order_year,
        extract(month from created_at) as order_month,
        extract(day from created_at) as order_day_of_month,
        
        cast(created_at as date) as created_date,

        --order numbers

        row_number() over (partition by buyeremail order by created_at) as order_seq_number,
        
        case
            when is_completed = 1
                then row_number() over (
                    partition by buyeremail, is_completed
                    order by created_at)
            else null
        end as completed_order_number

    from orders

),

order_numbers as (

    select

        *,
        
        case
            when completed_order_number = 1
                then 'new'
            else 'repeat'
        end as new_vs_repeat

    from fields
),

calculation_1 as (

--this CTE calculates values for a given user either for total values or
--for completed values only

    select

        *,
        
        --total values
        
        min(cast(created_at as timestamp)) over (partition by buyeremail) as first_order_date,
        
        count(*) over (partition by buyeremail)
            as lifetime_placed_orders,
        
        sum(coalesce(order_total, 0)) over (partition by buyeremail order
            by created_at rows between unbounded preceding and unbounded following) as lifetime_revenue,
        
        --completed order values

        min(case when is_completed = 1 then created_at else null end)
            over (partition by buyeremail) as first_completed_order_date,

        sum(case when is_completed = 1 then 1 else 0 end)
            over (partition by buyeremail order
            by created_at rows between unbounded preceding and unbounded following) as lifetime_completed_orders,

        sum(case when is_completed = 1 then order_total else 0 end)
            over (partition by buyeremail order
            by created_at rows between unbounded preceding and unbounded following) as lifetime_completed_revenue,
        
        --this creates a field needed to achieve the final value in the next CTE
        
        lag(created_at) over (partition by buyeremail,
            is_completed order by created_at) as previous_completed_order_date

    from order_numbers

),

date_diffs as (

    select

        *,

        case
            when created_at < first_completed_order_date then null
            else datediff('month', first_completed_order_date, created_at)
        end as months_from_first_completed_order,

        case
            when created_at < first_completed_order_date then null
            else datediff('week', first_completed_order_date, created_at)
        end as weeks_from_first_completed_order,

        case
            when created_at < first_completed_order_date then null
            else datediff('day', first_completed_order_date, created_at)
        end as days_from_first_completed_order,

        case
            when created_at <= first_completed_order_date then null
            else datediff('day', previous_completed_order_date, created_at)
        end as days_since_previous_completed_order,

        case
            when created_at < first_completed_order_date then null
            else datediff('day', first_completed_order_date, current_date)
        end as customer_age_days

    from calculation_1
    
),

final_calculations as (

    select

        *,

        case when lifetime_completed_orders = 1 then 'single_purchaser'
            when lifetime_completed_orders > 1 then 'repeat_purchaser'
            else 'non_purchaser'
        end as customer_type,

        sum(case when days_from_first_completed_order <= 30 and is_completed = 1
            then 1 else 0 end)
            over (partition by buyeremail order by created_at rows between unbounded preceding and unbounded following)
            as customer_first_30_day_completed_orders,

        sum(case when days_from_first_completed_order <= 30 and is_completed = 1
            then order_total else 0 end)
            over (partition by buyeremail order by created_at rows between unbounded preceding and unbounded following)
        as customer_first_30_day_revenue,

        sum(case when days_from_first_completed_order <= 60 and is_completed = 1
            then 1 else 0 end)
            over (partition by buyeremail order by created_at rows between unbounded preceding and unbounded following)
            as customer_first_60_day_completed_orders,

        sum(case when days_from_first_completed_order <= 60 and is_completed = 1
            then order_total else 0 end)
            over (partition by buyeremail order by created_at rows between unbounded preceding and unbounded following)
            as customer_first_60_day_revenue,

        sum(case when days_from_first_completed_order <= 90 and is_completed = 1
            then 1 else 0 end)
            over (partition by buyeremail order by created_at rows between unbounded preceding and unbounded following)
            as customer_first_90_day_completed_orders,

        sum(case when days_from_first_completed_order <= 90 and is_completed = 1
            then order_total else 0 end)
            over (partition by buyeremail order by created_at rows between unbounded preceding and unbounded following)
            as customer_first_90_day_revenue

    from date_diffs
    
)

select * from final_calculations