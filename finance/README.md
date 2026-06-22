# finance — consumer project

A **consumer** domain in the retail Mesh. Builds finance reporting on top of the
governed models published by the `platform` producer.

## Cross-project references

`dependencies.yml` declares the producer (`platform`); models use
`{{ ref('platform', '<model>') }}`.

## Models

| Model | Materialization | Built from | Purpose |
|-------|-----------------|-----------|---------|
| `fct_daily_revenue` | **materialized view** | `fct_sales` | Daily revenue / units / orders |
| `mart_b2b_orders` | table | `fct_orders` | B2B order economics: gross vs net booked |

### Why a materialized view here

`fct_daily_revenue` is an ordinary dbt model that materializes as a Databricks
**materialized view**. dbt manages its definition and refresh declaratively and
keeps it in the lineage graph — the same model file you'd write for a table, no
separate pipeline framework. This is the direct contrast to building the same
rollup as a standalone DLT pipeline.

## Run order

```bash
dbt deps
dbt build      # requires the platform producer deployed in dbt platform
```

> Cross-project `ref('platform', …)` resolves against the producer's production
> publication artifact in the dbt platform — deploy `platform` first (top-level
> [`LABS.md`](../LABS.md), Lab 3).
