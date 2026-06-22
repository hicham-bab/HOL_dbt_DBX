{{
    config(
        materialized='table',
        access='protected'
    )
}}

with ret_tickets as (

    select * from {{ ref('stg_retail__ret_tickets') }}

),

final as (

    select
        ticket_id,
        ret_customer_id,
        issue_type,
        ticket_status,
        created_at,

        -- TODO: verify status enum values against raw data
        case
            when lower(ticket_status) in ('closed', 'resolved') then false
            else true
        end as is_open

    from ret_tickets

)

select * from final
