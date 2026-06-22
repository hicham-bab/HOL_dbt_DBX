with source as (

    select * from {{ source('retail', 'loyalty_segments') }}

),

renamed as (

    select
        cast(loyalty_segment_id as string)          as loyalty_segment_id,
        cast(loyalty_segment_description as string)  as loyalty_segment_description,
        cast(unit_threshold as bigint)               as unit_threshold,

        cast(valid_from as timestamp)                as valid_from,
        cast(valid_to as timestamp)                  as valid_to,

        cast(_fivetran_synced as timestamp)          as _fivetran_synced

    from source
    where not coalesce(_fivetran_deleted, false)

)

select * from renamed
