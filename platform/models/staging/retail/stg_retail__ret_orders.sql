with source as (

    select * from {{ source('retail', 'ret_orders') }}

),

renamed as (

    select
        cast(id as bigint)                   as order_id,
        cast(order_user_id as bigint)        as ret_customer_id,

        cast(amount as decimal(18, 2))       as order_amount,
        cast(status as string)               as order_status,
        cast(cancel_return_reason as string) as cancel_return_reason,

        cast(created_at as timestamp)        as created_at,

        cast(_fivetran_synced as timestamp)  as _fivetran_synced

    from source
    where not coalesce(_fivetran_deleted, false)

)

select * from renamed
