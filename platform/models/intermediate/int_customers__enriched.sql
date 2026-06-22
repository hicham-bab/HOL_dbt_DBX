-- Enriches B2C customers with their loyalty segment description and threshold.
-- Customers may carry SCD validity windows from the source system, so we keep
-- only the current record per business key (latest valid_from).

with customers as (

    select * from {{ ref('stg_retail__customers') }}

),

loyalty_segments as (

    select * from {{ ref('stg_retail__loyalty_segments') }}

),

current_customers as (

    select *
    from customers
    -- TODO: verify dedup key — assumes customer_business_key identifies a customer
    --       across SCD versions and valid_from marks the latest record.
    qualify row_number() over (
        partition by customer_business_key
        order by valid_from desc nulls last
    ) = 1

),

joined as (

    select
        current_customers.customer_id,
        current_customers.customer_business_key,
        current_customers.customer_name,
        current_customers.loyalty_segment,
        loyalty_segments.loyalty_segment_description,
        loyalty_segments.unit_threshold,
        current_customers.region,
        current_customers.state,
        current_customers.city,
        current_customers.district,
        current_customers.postcode,
        current_customers.units_purchased
    from current_customers
    left join loyalty_segments
        on current_customers.loyalty_segment = loyalty_segments.loyalty_segment_id

)

select * from joined
