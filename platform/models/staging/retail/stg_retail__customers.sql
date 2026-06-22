with source as (

    select * from {{ source('retail', 'customers') }}

),

renamed as (

    select
        -- ids
        cast(id as bigint)              as customer_id,
        cast(customer_id as string)     as customer_business_key,

        -- attributes
        cast(customer_name as string)   as customer_name,
        cast(loyalty_segment as string) as loyalty_segment,

        -- address
        cast(street as string)          as street,
        cast(number as string)          as street_number,
        cast(unit as string)            as unit,
        cast(city as string)            as city,
        cast(district as string)        as district,
        cast(region as string)          as region,
        cast(state as string)           as state,
        cast(postcode as string)        as postcode,
        cast(lat as double)             as latitude,
        cast(lon as double)             as longitude,
        cast(ship_to_address as string) as ship_to_address,

        -- tax
        cast(tax_code as string)        as tax_code,
        cast(tax_id as string)          as tax_id,

        -- metrics
        cast(units_purchased as bigint) as units_purchased,

        -- scd validity (from source system)
        cast(valid_from as timestamp)   as valid_from,
        cast(valid_to as timestamp)     as valid_to,

        -- fivetran metadata
        cast(_fivetran_synced as timestamp) as _fivetran_synced

    from source
    where not coalesce(_fivetran_deleted, false)

)

select * from renamed
