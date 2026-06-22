with source as (

    select * from {{ source('retail', 'sales_orders') }}

),

renamed as (

    select
        cast(id as bigint)                       as sales_order_id,
        cast(order_number as string)             as order_number,

        -- order_datetime may land as epoch or ISO string -> normalize via macro
        {{ normalize_timestamp('order_datetime') }} as order_datetime,

        cast(customer_id as string)              as customer_id,
        cast(customer_name as string)            as customer_name,
        cast(number_of_line_items as bigint)     as number_of_line_items,

        -- semi-structured payloads kept intact; flattened downstream in intermediate
        -- TODO: verify ordered_products shape (array<struct> vs JSON string) against raw data
        ordered_products                         as ordered_products,
        clicked_items                            as clicked_items,
        promo_info                               as promo_info,

        cast(_fivetran_synced as timestamp)      as _fivetran_synced

    from source
    where not coalesce(_fivetran_deleted, false)

)

select * from renamed
