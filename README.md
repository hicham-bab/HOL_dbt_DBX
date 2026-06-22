# retail_analytics — dbt Mesh on Databricks (hands-on lab)

A production-style **dbt Mesh** for the *Fivetran + dbt on Databricks* hands-on
lab. Raw retail data is landed by Fivetran into Databricks Unity Catalog. One
**producer** project governs and publishes contracted interface models; two
**consumer** domains build their own analytics on top using cross-project
references — the dbt pattern for scaling analytics across teams on a lakehouse.

> **Doing the lab?** Start with **[`LABS.md`](LABS.md)** — a step-by-step guide
> from zero to a working Mesh, contracts, materialized view, unit tests, and
> semantic layer.

## The three projects

| Project | Role | Profile | What it owns |
|---------|------|---------|--------------|
| [`platform/`](platform/) | **Producer** | `platform` | Sources, staging, intermediate, marts, snapshot, semantic layer. Publishes 4 **public, contracted** interface models. |
| [`marketing/`](marketing/) | **Consumer** | `marketing` | Loyalty & regional marts built from `platform`'s public models; dashboard exposure. |
| [`finance/`](finance/) | **Consumer** | `finance` | Daily revenue (materialized view) + B2B order economics, built from `platform`. |

Each project is an independent dbt project (its own `dbt_project.yml`). Consumers
declare the producer in `dependencies.yml` and reference its models with
`{{ ref('platform', '<model>') }}`.

## Public interface (the Mesh contract)

`platform` exposes exactly four models as `access: public` with **enforced data
contracts** and **Unity Catalog primary/foreign keys**. Everything else is
`protected` (internal to the producer).

| Public model | Grain | Key constraints |
|--------------|-------|-----------------|
| `dim_customers` | one row per current B2C customer | PK `customer_business_key` |
| `fct_sales` | one row per order line item (incremental) | PK `sales_order_line_id`, FK → `dim_customers` |
| `fct_orders` | one row per B2B order | PK `order_id` |
| `dim_loyalty_segments` | one row per loyalty segment | PK `loyalty_segment_id` |

## Topology

```
 PLATFORM (producer)                                  CONSUMERS
 ─────────────────────────────────────────────       ───────────────────────────────────────

 Fivetran → UC sources                                MARKETING  (ref('platform', …))
   retail.customers, loyalty_segments,                  mart_customer_loyalty      ← dim_customers + fct_sales
   ret_customers, ret_orders, ret_tickets,              mart_segment_region_rollup ← + dim_loyalty_segments
   sales_orders                                         └─ [exposure] customer_loyalty_dashboard
        │
        ▼ staging (views) → intermediate (ephemeral)  FINANCE    (ref('platform', …))
        ▼ marts (Delta tables)                           fct_daily_revenue  ← fct_sales   [MATERIALIZED VIEW]
   ┌──────────────── public, contracted ───────────┐     mart_b2b_orders    ← fct_orders
   │ dim_customers   fct_sales                      │
   │ fct_orders      dim_loyalty_segments           │──────────► consumed across the Mesh
   └────────────────────────────────────────────────┘
     fct_support_tickets        (protected)
     customers_snapshot (SCD2)  semantic_models → metrics
```

## What this lab demonstrates (dbt alongside Databricks-native tooling)

- **dbt Mesh** — governed cross-project references, not copy-paste SQL between teams.
- **Data contracts + Unity Catalog PK/FK** — the column shape and keys are enforced
  at build time and surfaced in Unity Catalog.
- **Materialized view** — `finance.fct_daily_revenue` is a Databricks MV defined as
  an ordinary dbt model: declarative, versioned, fully in the lineage graph.
- **Unit tests** — transformation logic (the nested-payload explode, SCD dedup) is
  tested against mocked inputs, separate from data quality tests.
- **Semantic layer** — governed metrics (`total_revenue`, `avg_basket_value`, …)
  defined once on the producer.
- **Lineage & docs** — one DAG across sources, three projects, snapshot, and exposure.

## Quick start

Full walkthrough is in [`LABS.md`](LABS.md). The short version (run the producer first):

```bash
cd platform
dbt deps
dbt build              # staging → intermediate → marts (+ snapshot, tests)
dbt test --select test_type:unit   # unit tests only
```

Then build a consumer (requires the producer deployed in the dbt platform so the
cross-project `ref('platform', …)` resolves):

```bash
cd ../marketing && dbt deps && dbt build
cd ../finance   && dbt deps && dbt build
```

Run targets and credentials are covered in `LABS.md` and each project's
`profiles.yml.example`.
