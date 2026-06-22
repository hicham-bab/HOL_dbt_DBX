{{
    config(
        materialized='table',
        access='public',
        group='retail',
        contract={'enforced': true}
    )
}}

with loyalty_segments as (

    select * from {{ ref('stg_retail__loyalty_segments') }}

),

final as (

    select
        cast(loyalty_segment_id as string)           as loyalty_segment_id,
        cast(loyalty_segment_description as string)  as loyalty_segment_description,
        cast(unit_threshold as bigint)               as unit_threshold
    from loyalty_segments

)

select * from final
