{{
    config(
        materialized='table',
        access='public',
        group='retail',
        contract={'enforced': true}
    )
}}

with ret_orders as (

    select * from {{ ref('stg_retail__ret_orders') }}

),

final as (

    select
        cast(order_id as bigint)              as order_id,
        cast(ret_customer_id as bigint)       as ret_customer_id,
        cast(order_amount as decimal(18, 2))  as order_amount,
        cast(order_status as string)          as order_status,
        cast(cancel_return_reason as string)  as cancel_return_reason,
        cast(created_at as timestamp)         as created_at,

        -- TODO: verify status enum values against raw data
        cast(case when lower(order_status) = 'cancelled' then true else false end as boolean) as is_cancelled,
        cast(case when lower(order_status) = 'returned'  then true else false end as boolean) as is_returned,
        cast(case
            when cancel_return_reason is not null and trim(cancel_return_reason) <> ''
            then true else false
        end as boolean) as has_cancel_return_reason

    from ret_orders

)

select * from final
