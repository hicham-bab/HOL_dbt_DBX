{{ config(materialized='table') }}

-- Finance domain. B2B order economics built from the governed platform.fct_orders
-- interface model: gross vs net booked amount after cancellations/returns.

with orders as (

    select * from {{ ref('platform', 'fct_orders') }}

),

final as (

    select
        cast(created_at as date)                                            as order_date,
        order_status,
        count(*)                                                            as order_count,
        sum(order_amount)                                                   as gross_amount,
        sum(case when is_cancelled or is_returned then 0 else order_amount end) as net_booked_amount,
        sum(case when is_cancelled then 1 else 0 end)                       as cancelled_orders,
        sum(case when is_returned then 1 else 0 end)                        as returned_orders
    from orders
    group by cast(created_at as date), order_status

)

select * from final
