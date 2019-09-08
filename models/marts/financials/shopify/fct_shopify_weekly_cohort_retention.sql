with customers as (

    select *
    from {{ ref('orders_xf') }}
    
), 

second as (

    select 
        customer_id,
        created_at as second_order_date,
        first_order_date,
        date_trunc(week,first_order_date) as cohort_week,
        days_from_first_completed_order as days_to_second_purchase,
        customer_age_days,
        case when days_from_first_completed_order <= 7 
            and customer_age_days >= 7 then 1 else 0 end as "repeat_1w_rate",
        case when days_from_first_completed_order <= 14 
            and customer_age_days >= 14 then 1 else 0 end as "repeat_2w_rate",
        case when days_from_first_completed_order <= 28 
            and customer_age_days >= 28 then 1 else 0 end as "repeat_4w_rate",
        case when days_from_first_completed_order <= 56 
            and customer_age_days >= 56 then 1 else 0 end as "repeat_8w_rate",
        case when days_from_first_completed_order <= 84 
            and customer_age_days >= 84 then 1 else 0 end as "repeat_12w_rate",
        case when days_from_first_completed_order <= 168 
            and customer_age_days >= 168 then 1 else 0 end as "repeat_24w_rate"
    from {{ ref('orders_xf') }}
    where order_seq_number = 2
    order by customer_id, order_seq_number
    
), 

cohort_sizes as (

    select 
      date_trunc(week,first_order_date) as cohort_week,
      count(distinct customer_id) as cohort_size
    from customers
    group by 1
    order by 1 desc

)

select 
    s.cohort_week,
    c.cohort_size,
    sum("repeat_1w_rate")/c.cohort_size*100 as repeat_1w_rate,
    sum("repeat_2w_rate")/c.cohort_size*100 as repeat_2w_rate,
    sum("repeat_4w_rate")/c.cohort_size*100 as repeat_4w_rate,
    sum("repeat_8w_rate")/c.cohort_size*100 as repeat_8w_rate,
    sum("repeat_12w_rate")/c.cohort_size*100 as repeat_12w_rate,
    sum("repeat_24w_rate")/c.cohort_size*100 as repeat_24w_rate
from second s
left join cohort_sizes c on c.cohort_week = s.cohort_week
group by 1,2
order by 1 asc