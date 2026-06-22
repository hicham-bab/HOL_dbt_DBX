{#
    normalize_timestamp(column_name)

    Returns a TIMESTAMP regardless of whether the source column arrives as an
    ISO-8601 string / native timestamp or as an epoch value. Fivetran can land
    PostgreSQL timestamp columns inconsistently depending on the source type, so
    this macro defends against both shapes.

    - If the value parses directly as a timestamp (native timestamp or ISO string),
      that result is used.
    - Otherwise it is treated as epoch SECONDS (Databricks interprets an integer
      cast to timestamp as seconds since 1970-01-01).

    -- TODO: verify against raw data whether sales_orders.order_datetime is epoch
    --       seconds, epoch milliseconds, or an ISO timestamp. If milliseconds,
    --       divide the bigint by 1000 before the cast below.
#}
{% macro normalize_timestamp(column_name) %}
    coalesce(
        try_cast({{ column_name }} as timestamp),
        cast(try_cast({{ column_name }} as bigint) as timestamp)
    )
{% endmacro %}
