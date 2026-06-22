{{
    config(
        materialized='table',
        access='public',
        group='retail',
        contract={'enforced': true}
    )
}}

with enriched as (

    select * from {{ ref('int_customers__enriched') }}

),

final as (

    select
        cast(customer_id as bigint)                  as customer_id,
        cast(customer_business_key as string)        as customer_business_key,
        cast(customer_name as string)                as customer_name,
        cast(loyalty_segment as string)              as loyalty_segment,
        cast(loyalty_segment_description as string)  as loyalty_segment_description,
        cast(region as string)                       as region,
        cast(state as string)                        as state,
        cast(city as string)                         as city,
        cast(units_purchased as bigint)              as units_purchased,
        cast(unit_threshold as bigint)               as unit_threshold
    from enriched

)

select * from final
