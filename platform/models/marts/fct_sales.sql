{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='sales_order_line_id',
        on_schema_change='append_new_columns',
        access='public',
        group='retail',
        contract={'enforced': true}
    )
}}

with order_items as (

    select * from {{ ref('int_sales__order_items') }}

),

final as (

    select
        cast({{ dbt_utils.generate_surrogate_key(['order_number', 'product_id']) }} as string) as sales_order_line_id,
        cast(sales_order_id as bigint)  as sales_order_id,
        cast(order_number as string)    as order_number,
        cast(customer_id as string)     as customer_id,
        cast(product_id as string)      as product_id,
        cast(product_name as string)    as product_name,
        cast(order_datetime as timestamp) as order_datetime,
        cast(unit_price as double)      as unit_price,
        cast(quantity as bigint)        as quantity,
        cast(line_revenue as double)    as line_revenue
    from order_items

)

select * from final

{% if is_incremental() %}
-- Only process orders newer than what has already been loaded.
where order_datetime > (select coalesce(max(order_datetime), cast('1900-01-01' as timestamp)) from {{ this }})
{% endif %}
