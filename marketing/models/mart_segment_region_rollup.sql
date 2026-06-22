{{ config(materialized='table') }}

-- Region-level performance rollup for marketing. Demonstrates consuming two
-- governed platform models (a fact and a dimension) and joining a third for
-- segment context. Powers regional campaign targeting.

with sales as (

    select * from {{ ref('platform', 'fct_sales') }}

),

customers as (

    select * from {{ ref('platform', 'dim_customers') }}

),

segments as (

    select * from {{ ref('platform', 'dim_loyalty_segments') }}

),

joined as (

    select
        customers.region,
        customers.loyalty_segment,
        segments.unit_threshold,
        customers.units_purchased,
        sales.customer_id,
        sales.line_revenue
    from sales
    inner join customers
        on sales.customer_id = customers.customer_business_key
    left join segments
        on customers.loyalty_segment = segments.loyalty_segment_id

),

final as (

    select
        region,
        count(distinct customer_id)                                              as active_customers,
        count(distinct loyalty_segment)                                          as distinct_segments,
        sum(line_revenue)                                                        as total_revenue,
        count(distinct case when units_purchased >= unit_threshold
              then customer_id end)                                              as customers_meeting_threshold
    from joined
    group by 1

)

select * from final
