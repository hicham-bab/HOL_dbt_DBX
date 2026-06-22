# marketing — consumer project

A **consumer** domain in the retail Mesh. Builds marketing analytics on top of
the governed models published by the `platform` producer — without copying or
re-deriving that logic.

## Cross-project references

`dependencies.yml` declares the producer:

```yaml
projects:
  - name: platform
```

Models then reference governed models directly:

```sql
select * from {{ ref('platform', 'dim_customers') }}
select * from {{ ref('platform', 'fct_sales') }}
select * from {{ ref('platform', 'dim_loyalty_segments') }}
```

## Models

| Model | Built from | Purpose |
|-------|-----------|---------|
| `mart_customer_loyalty` | `dim_customers`, `fct_sales` | Revenue, basket value, units by loyalty segment + region |
| `mart_segment_region_rollup` | `dim_customers`, `fct_sales`, `dim_loyalty_segments` | Region performance + threshold attainment |

`customer_loyalty_dashboard` (exposure) documents the downstream BI dependency.

## Run order

```bash
dbt deps
dbt build      # requires the platform producer deployed in dbt platform
```

> Cross-project `ref('platform', …)` resolves against the producer's production
> publication artifact in the dbt platform. If `platform` has not been deployed,
> the build fails while downloading that artifact — deploy the producer first
> (top-level [`LABS.md`](../LABS.md), Lab 3).
