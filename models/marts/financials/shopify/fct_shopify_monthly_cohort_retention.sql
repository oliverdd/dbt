with customers as (

    select *
    from {{ ref('orders_xf') }}

), 

second as (

    select 
        customer_id,
        created_at as second_order_date,
        first_order_date,
        date_trunc(month,first_order_date) as cohort_month,
        days_from_first_completed_order as days_to_second_purchase,
        customer_age_days,
        case when days_from_first_completed_order <= 30 
            and customer_age_days >= 30 then 1 else 0 end as "repeat_30d_rate",
        case when days_from_first_completed_order <= 60 
            and customer_age_days >= 60 then 1 else 0 end as "repeat_60d_rate",
        case when days_from_first_completed_order <= 90 
            and customer_age_days >= 88 then 1 else 0 end as "repeat_90d_rate",
        case when days_from_first_completed_order <= 180 
            and customer_age_days >= 180 then 1 else 0 end as "repeat_180d_rate",
        case when days_from_first_completed_order <= 360 
            and customer_age_days >= 360 then 1 else 0 end as "repeat_360d_rate"
    from {{ ref('orders_xf') }}
    where order_seq_number = 2
    order by customer_id, order_seq_number

), 

cohort_sizes as (

    select 
      date_trunc(month,first_order_date) as cohort_month,
      count(distinct customer_id) as cohort_size
    from {{ ref('orders_xf') }}
    group by 1
    order by 1 desc

)

select 
    s.cohort_month,
    c.cohort_size,
    sum("repeat_30d_rate")/c.cohort_size*100 as repeat_30d_rate,
    sum("repeat_60d_rate")/c.cohort_size*100 as repeat_60d_rate,
    sum("repeat_90d_rate")/c.cohort_size*100 as repeat_90d_rate,
    sum("repeat_180d_rate")/c.cohort_size*100 as repeat_180d_rate,
    sum("repeat_360d_rate")/c.cohort_size*100 as repeat_360d_rate
from second s
left join cohort_sizes c on c.cohort_month = s.cohort_month
group by 1,2
order by 1 asc