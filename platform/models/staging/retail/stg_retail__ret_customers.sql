with source as (

    select * from {{ source('retail', 'ret_customers') }}

),

renamed as (

    select
        cast(id as bigint)                  as ret_customer_id,
        cast(name as string)                as contact_name,
        cast(company_name as string)        as company_name,
        cast(email as string)               as email,
        cast(region as string)              as region,
        cast(customer_start_date as date)   as customer_start_date,

        cast(_fivetran_synced as timestamp) as _fivetran_synced

    from source
    where not coalesce(_fivetran_deleted, false)

)

select * from renamed
