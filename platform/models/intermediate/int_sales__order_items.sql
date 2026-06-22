-- Explodes the nested ordered_products payload from sales orders into one row
-- per product line item, and computes line-level revenue (price * qty).
--
-- TODO: verify the ordered_products struct field names against raw data. This
--       assumes an array<struct<id, name, price, qty>>. If the column instead
--       lands as a JSON string, replace the lateral view with:
--           lateral view explode(from_json(
--               ordered_products,
--               'array<struct<id:string,name:string,price:double,qty:bigint>>'
--           )) exploded as item

with sales_orders as (

    select * from {{ ref('stg_retail__sales_orders') }}

),

line_items as (

    select
        sales_orders.sales_order_id,
        sales_orders.order_number,
        sales_orders.customer_id,
        sales_orders.order_datetime,

        cast(item.id as string)     as product_id,
        cast(item.name as string)   as product_name,
        cast(item.price as double)  as unit_price,
        cast(item.qty as bigint)    as quantity

    from sales_orders
    lateral view explode(sales_orders.ordered_products) exploded as item

),

final as (

    select
        sales_order_id,
        order_number,
        customer_id,
        order_datetime,
        product_id,
        product_name,
        unit_price,
        quantity,
        unit_price * quantity as line_revenue
    from line_items

)

select * from final
