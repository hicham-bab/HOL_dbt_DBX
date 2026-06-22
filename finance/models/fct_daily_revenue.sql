{{
    config(
        materialized='materialized_view'
    )
}}

-- Finance domain. Daily revenue rollup built as a Databricks MATERIALIZED VIEW:
-- dbt manages the MV definition and refresh declaratively, with full lineage
-- back to the governed platform.fct_sales — the same model file you'd write for
-- a table, no separate pipeline framework required.

with sales as (

    select * from {{ ref('platform', 'fct_sales') }}

),

final as (

    select
        cast(order_datetime as date)        as order_date,
        count(distinct order_number)        as order_count,
        sum(quantity)                       as total_units,
        sum(line_revenue)                   as total_revenue,
        sum(line_revenue)
            / nullif(count(distinct order_number), 0) as avg_order_value
    from sales
    group by cast(order_datetime as date)

)

select * from final
