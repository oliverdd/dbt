with stagged_events as (

    select * from {{ref('stg_events')}}

),

relevant_events as (

    select
        domain_userid,
        user_id,
        collector_tstamp

    from stagged_events
    where user_id is not null
      and domain_userid is not null
      and collector_tstamp is not null

),

is_email as (

    select
        *,
        case
            when user_id like '%@%' then 1
            else 0
        end as is_email_flag
    from relevant_events

),

prep as (

    select distinct
        domain_userid,
        is_email_flag,
        last_value(user_id)
            over (partition by domain_userid, is_email_flag 
                order by collector_tstamp rows 
                between unbounded preceding and unbounded following) as user_id,

        max(collector_tstamp)
            over (partition by domain_userid) as max_tstamp

    from is_email

),

-- ensure we're not duplicating domain_userid's
dedupe as (

    select 
        *,
        row_number() over (partition by domain_userid order by max_tstamp desc,
            is_email_flag desc) as idx
    from prep

)

select * from dedupe where idx = 1