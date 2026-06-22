{{ config(materialized='table') }}

-- Marketing domain mart. Consumes the governed, contracted interface models
-- from the `platform` producer project via dbt Mesh cross-project refs.
-- Revenue, basket value and units aggregated by loyalty segment and region.

with sales as (

    select * from {{ ref('platform', 'fct_sales') }}

),

customers as (

    select * from {{ ref('platform', 'dim_customers') }}

),

sales_with_segment as (

    select
        customers.loyalty_segment,
        customers.loyalty_segment_description,
        customers.region,
        sales.order_number,
        sales.customer_id,
        sales.quantity,
        sales.line_revenue
    from sales
    inner join customers
        on sales.customer_id = customers.customer_business_key

),

final as (

    select
        loyalty_segment,
        loyalty_segment_description,
        region,

        count(distinct customer_id)                                 as customer_count,
        count(distinct order_number)                                as order_count,
        sum(quantity)                                               as total_units,
        sum(line_revenue)                                           as total_revenue,
        sum(line_revenue) / nullif(count(distinct order_number), 0) as avg_basket_value
    from sales_with_segment
    group by 1, 2, 3

)

select * from final
