{% snapshot customers_snapshot %}

{{
    config(
        unique_key='id',
        strategy='check',
        check_cols=[
            'loyalty_segment',
            'street',
            'number',
            'unit',
            'city',
            'district',
            'region',
            'state',
            'postcode'
        ],
        invalidate_hard_deletes=true
    )
}}

-- SCD2 history of B2C customers. Tracks changes to loyalty segment and address.
-- Snapshot reads directly from the raw source so it captures changes regardless
-- of staging logic. Soft-deleted Fivetran rows are excluded.
select *
from {{ source('retail', 'customers') }}
where not coalesce(_fivetran_deleted, false)

{% endsnapshot %}
