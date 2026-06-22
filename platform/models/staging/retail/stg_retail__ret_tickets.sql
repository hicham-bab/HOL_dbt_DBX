with source as (

    select * from {{ source('retail', 'ret_tickets') }}

),

renamed as (

    select
        cast(id as bigint)                  as ticket_id,
        cast(ticket_user_id as bigint)      as ret_customer_id,

        cast(issue_type as string)          as issue_type,
        cast(status as string)              as ticket_status,

        cast(created_at as timestamp)       as created_at,

        cast(_fivetran_synced as timestamp) as _fivetran_synced

    from source
    where not coalesce(_fivetran_deleted, false)

)

select * from renamed
