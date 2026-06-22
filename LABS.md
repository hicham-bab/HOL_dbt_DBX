# Hands-on lab: dbt Mesh on Databricks

Build a governed, multi-team analytics platform on Databricks with dbt — sources
to marts, enforced data contracts with Unity Catalog keys, a three-project dbt
Mesh, a materialized view, unit tests, and a semantic layer. Each lab is
self-contained and builds on the previous one.

**Time:** ~90 minutes • **You'll finish with:** three connected dbt projects, a
contract you can break and fix, cross-project lineage, and governed metrics.

---

## Concepts in 60 seconds

If you're coming from Databricks-native tooling, here's the dbt vocabulary used
below and the nearest Databricks equivalent.

| dbt concept | What it is | Nearest Databricks-native analogue |
|-------------|-----------|-------------------------------------|
| **model** | A `SELECT` in a `.sql` file. dbt wraps it in `CREATE TABLE/VIEW` and figures out run order. | A notebook cell / a DLT `@dlt.table` |
| **`ref()` / `source()`** | How models point at each other and at raw tables. dbt builds the DAG from these. | Manual table names + task dependencies |
| **materialization** | How a model is persisted: `view`, `table`, `incremental`, `materialized_view`. | Delta table vs view vs DLT live table |
| **data contract** | An enforced promise about a model's columns, types, and keys. | DLT expectations (data), but contracts govern *schema/shape* |
| **test** | `data tests` validate rows; `unit tests` validate SQL logic against mocked inputs. | DLT expectations / manual assertions |
| **Mesh** | Many dbt projects referencing each other's *public* models across teams. | Separate workflows sharing tables by name |
| **semantic layer** | Metrics defined once, queried consistently everywhere. | Metric views / repeated SQL in dashboards |
| **exposure** | A documented downstream consumer (e.g. a dashboard) in the lineage graph. | (no direct equivalent) |

> Throughout, **dbt vs native** callouts highlight what dbt adds over building
> the same thing with notebooks or DLT alone.

---

## What you'll build

```
 PLATFORM (producer)                          CONSUMERS
 ──────────────────────────────────           ──────────────────────────────────
 Fivetran → Unity Catalog sources             MARKETING   ref('platform', …)
   retail.{customers, loyalty_segments,         mart_customer_loyalty
   ret_customers, ret_orders,                    mart_segment_region_rollup
   ret_tickets, sales_orders}                    └─ [exposure] loyalty dashboard
        │
        ▼ staging (views)                      FINANCE     ref('platform', …)
        ▼ intermediate (ephemeral)               fct_daily_revenue  [MATERIALIZED VIEW]
        ▼ marts (Delta tables)                   mart_b2b_orders
   ┌──── public + contracted (PK/FK) ────┐
   │ dim_customers   fct_sales           │ ───────► consumed across the Mesh
   │ fct_orders      dim_loyalty_segments│
   └──────────────────────────────────────┘
     fct_support_tickets (protected)
     customers_snapshot (SCD2)   semantic metrics
```

Three projects in this repo: [`platform/`](platform/) (producer),
[`marketing/`](marketing/) and [`finance/`](finance/) (consumers).

---

## Prerequisites

1. **Databricks**: a workspace with **Unity Catalog** and a **SQL warehouse**
   you can run queries on. Note your catalog name (the labs assume `main`).
2. **Raw data landed by Fivetran**: six tables in a Unity Catalog schema
   (default in this repo: `hicham_babahmed_retail`). Each carries
   `_fivetran_synced` and `_fivetran_deleted`:
   `customers, loyalty_segments, ret_customers, ret_orders, ret_tickets, sales_orders`.
3. **A dbt platform account** with permission to create projects and connect to
   Databricks. (Cross-project references in Lab 3 resolve through the platform.)
4. This repository, pushed to a git remote your dbt platform account can read
   (GitHub/GitLab/Azure DevOps).

> No dbt platform access? You can still do Labs 1, 2, 4, 5, 6 against a single
> project locally with the Fusion CLI — see [Appendix A](#appendix-a--local-cli-path).

---

## Lab 0 — Connect Databricks and create the producer project

**Goal:** stand up the `platform` project in the dbt platform, connected to your
Databricks SQL warehouse.

1. In the dbt platform, create a **connection** to Databricks:
   - Type: **Databricks**
   - **Server hostname** and **HTTP path** of your SQL warehouse
   - Authentication: a **token** (or OAuth, per your workspace policy)
   - **Catalog**: `main` (or yours)
2. Create a **project** named **`platform`**. Point it at this repo and set the
   **project subdirectory** to `platform`.

   > Each dbt project has its own `dbt_project.yml`. Because this repo holds
   > three projects, every project's "subdirectory" setting matters:
   > `platform`, `marketing`, `finance`.
3. Set the **development credentials** so your dev schema is personal — e.g.
   schema `dbt_<your_initials>`. The repo never hardcodes schemas; dbt routes
   output to whatever schema your environment defines.
4. Set the raw-data location. In `platform/dbt_project.yml`:
   ```yaml
   vars:
     raw_catalog: main
     raw_schema: hicham_babahmed_retail   # ← your Fivetran destination schema
   ```
   Or override at run time: `dbt build --vars '{raw_catalog: main, raw_schema: my_schema}'`.
5. Open the dbt **IDE / Cloud CLI** for the `platform` project and run:
   ```bash
   dbt deps
   dbt debug      # confirms the warehouse connection
   ```
   `dbt debug` should report a successful connection. If not, fix the connection
   before continuing.

> **dbt vs native:** you just declared *where* compute and data live once.
> Every model in the project inherits it — no per-notebook connection setup.

---

## Lab 1 — Build the producer

**Goal:** transform raw Fivetran tables into governed marts and see the DAG.

1. Build everything:
   ```bash
   dbt build
   ```
   `dbt build` runs models, snapshots, and tests **in dependency order**, derived
   automatically from `ref()`/`source()` — you never wrote a task graph.

2. Inspect what ran. The layers (see [`platform/README.md`](platform/README.md)):
   - **staging** (`stg_retail__*`, views): rename/cast columns, drop
     `_fivetran_deleted` rows.
   - **intermediate** (`int_*`, ephemeral — inlined into downstream SQL, no table
     created): `int_sales__order_items` explodes the nested `ordered_products`
     payload into one row per line item; `int_customers__enriched` keeps the
     latest record per customer and joins loyalty data.
   - **marts** (Delta tables): `dim_customers`, `fct_sales` (incremental),
     `fct_orders`, `dim_loyalty_segments`, `fct_support_tickets`.

3. Look at a model and its compiled SQL in the IDE. The model file is just a
   `SELECT`; dbt generated the `CREATE` DDL, the schema, and the merge logic.

4. Check the incremental model. `fct_sales` is `materialized='incremental'` with
   `incremental_strategy='merge'`. Run `dbt build` again — the second run only
   merges orders newer than what's already loaded (see the `is_incremental()`
   block in `platform/models/marts/fct_sales.sql`).

   > **dbt vs native:** the same model file is a full rebuild on first run and an
   > incremental Delta `MERGE` afterward. No separate streaming/batch code paths.

5. Check **source freshness** (uses Fivetran's `_fivetran_synced`):
   ```bash
   dbt source freshness
   ```

6. Generate docs and open the **lineage graph**:
   ```bash
   dbt docs generate
   ```
   In Unity Catalog, open `dim_customers` and note the **table and column
   comments** — dbt pushed your descriptions there via `persist_docs`.

   > **dbt vs native:** documentation is versioned with the code and propagates
   > into Unity Catalog automatically, instead of being maintained separately.

---

## Lab 2 — Data contracts and Unity Catalog keys

**Goal:** see an enforced contract reject a bad change, and find PK/FK in Unity
Catalog.

The four public models declare **enforced contracts** — column names, data
types, and constraints are checked at build time. See
`platform/models/marts/_marts__models.yml`.

### 2a. Break a contract on purpose

1. Open `platform/models/marts/fct_sales.sql`. Find:
   ```sql
   cast(quantity as bigint) as quantity,
   ```
   Change it to a deliberately wrong type:
   ```sql
   cast(quantity as string) as quantity,
   ```
2. Run:
   ```bash
   dbt build --select fct_sales
   ```
   The build **fails** before writing the table: the contract says `quantity` is
   `bigint`, the model now produces `string`. dbt names the offending column.
3. Revert the change (`string` → `bigint`) and re-run — it passes.

   > **dbt vs native:** the contract caught a breaking schema change *before* it
   > reached the table that `marketing` and `finance` depend on. This is the
   > guardrail that makes cross-team consumption safe.

### 2b. See the keys in Unity Catalog

The contracts declare Databricks **PRIMARY KEY / FOREIGN KEY** constraints:

- `dim_customers` PK = `customer_business_key`
- `fct_sales` PK = `sales_order_line_id`, FK `customer_id` → `dim_customers`
- `fct_orders` PK = `order_id`
- `dim_loyalty_segments` PK = `loyalty_segment_id`

After `dbt build`, inspect them in Databricks:
```sql
DESCRIBE EXTENDED main.<your_schema>.fct_sales;
-- or, in the Catalog Explorer, open fct_sales → "Constraints"
```
You'll see the PK and the FK to `dim_customers`. These are informational
(RELY-style) constraints the optimizer and BI tools can use.

> **dbt vs native:** keys are declared once alongside the contract and applied to
> Unity Catalog on every build — they can't drift from the model.

---

## Lab 3 — Build the Mesh (cross-project references)

**Goal:** stand up two consumer projects that build on `platform`'s public
models without copying logic.

### 3a. Deploy the producer

Cross-project `ref('platform', …)` resolves against the producer's **production
publication artifact** in the dbt platform. So `platform` must run a production
job first.

1. In the `platform` project, create a **deployment environment** (target
   `prod`, schema e.g. `analytics`).
2. Create and run a **job** in that environment: `dbt build`.
   This publishes the artifact that lists `platform`'s public models.

   > If a consumer build later errors with *"Failed to download publication
   > artifact … 404"*, it means this step hasn't completed — the producer isn't
   > deployed yet.

### 3b. Create the consumer projects

For **`marketing`** and then **`finance`**, repeat:

1. Create a dbt platform **project** pointing at this repo, subdirectory
   `marketing` (then `finance`).
2. Confirm `dependencies.yml` declares the producer (already in the repo):
   ```yaml
   projects:
     - name: platform
   ```
3. Open the consumer's models and note the cross-project refs, e.g.
   `marketing/models/mart_customer_loyalty.sql`:
   ```sql
   select * from {{ ref('platform', 'dim_customers') }}
   select * from {{ ref('platform', 'fct_sales') }}
   ```
4. Build:
   ```bash
   dbt deps
   dbt build
   ```

### 3c. See cross-project lineage

In the dbt platform's **Explorer**, view the lineage across all three projects:
sources → `platform` marts → `marketing`/`finance` marts → the exposure. One
graph, three teams.

> **dbt vs native:** consumers depend on a **governed, contracted** interface,
> not on raw table names. If the producer tries to break `fct_sales`'s shape
> (Lab 2a), the contract stops it — so consumers don't silently break.

> **Try it:** in a consumer, reference a `platform` model that is **not** public
> (e.g. `fct_support_tickets`). dbt blocks it — `access: protected` means it's
> internal to the producer. Only the four public models cross the Mesh boundary.

---

## Lab 4 — Materialized view (and the DLT contrast)

**Goal:** ship a Databricks materialized view as a plain dbt model.

`finance/models/fct_daily_revenue.sql` is `materialized='materialized_view'`:

```sql
{{ config(materialized='materialized_view') }}
select cast(order_datetime as date) as order_date,
       count(distinct order_number) as order_count,
       sum(line_revenue)            as total_revenue
from {{ ref('platform', 'fct_sales') }}
group by cast(order_datetime as date)
```

1. Build it:
   ```bash
   dbt build --select fct_daily_revenue
   ```
   dbt issues `CREATE MATERIALIZED VIEW` and manages refresh.
2. Confirm in Databricks:
   ```sql
   DESCRIBE EXTENDED main.<finance_schema>.fct_daily_revenue;
   SELECT * FROM main.<finance_schema>.fct_daily_revenue ORDER BY order_date DESC LIMIT 10;
   ```

> **dbt vs native:** this is the same model file you'd write for a table — only
> the `materialized` config changes. It stays in the dbt DAG with full lineage
> back to `platform.fct_sales`, is code-reviewed and versioned, and switches to a
> table or incremental model by changing one line. Building the equivalent as a
> standalone DLT pipeline means a separate framework, separate lineage, and
> separate review.

---

## Lab 5 — Unit tests (test the SQL, not just the data)

**Goal:** validate transformation logic against mocked inputs in seconds.

`platform/models/intermediate/_int__unit_tests.yml` defines two unit tests:

- **`explode_order_items_…`** — feeds one order with a 2-item `ordered_products`
  array and asserts it fans out to two rows with correct `line_revenue`.
- **`enrich_customers_…`** — feeds two SCD versions of one customer and asserts
  the latest survives and the loyalty join is correct.

1. Run only the unit tests:
   ```bash
   dbt test --select test_type:unit
   ```
   These build no tables and read no warehouse data — they run the SQL against
   the mocked rows in the YAML.

2. **Make one fail.** In `platform/models/intermediate/int_sales__order_items.sql`,
   change the revenue calculation:
   ```sql
   unit_price * quantity as line_revenue
   ```
   to
   ```sql
   unit_price + quantity as line_revenue   -- wrong on purpose
   ```
   Re-run `dbt test --select test_type:unit`. The explode test fails with
   expected-vs-actual `line_revenue`. Revert the change.

> **dbt vs native:** unit tests catch logic regressions (a bad join, a wrong
> formula) on every change, separate from data-quality tests. DLT expectations
> validate the *data*; unit tests validate the *transformation*.

---

## Lab 6 — Semantic layer and exposures

**Goal:** define metrics once and query them consistently.

`platform/semantic_models/sem_retail.yml` defines semantic models over
`fct_sales` and `dim_customers`, plus metrics: `total_revenue`, `total_units`,
`avg_basket_value`, `revenue_per_customer`.

1. With the dbt **Semantic Layer** enabled for the `platform` project, query a
   metric (via the dbt platform's metrics query tool, the MCP server, or BI
   integration), e.g. `total_revenue` grouped by `customer__region`.
2. Compare with the marketing mart `mart_customer_loyalty`, which computes
   revenue with hand-written SQL. The semantic layer makes that definition
   reusable and consistent across every consumer and dashboard.
3. Open the **exposure** `customer_loyalty_dashboard`
   (`marketing/models/_marketing__exposures.yml`). In Explorer it shows the BI
   dashboard as a downstream node — so you can trace a dashboard all the way
   back to raw Fivetran tables, across projects.

> **dbt vs native:** one governed definition of "revenue" instead of the same
> aggregation re-implemented in every dashboard and notebook.

---

## You're done

You built and governed a three-team analytics platform on Databricks:

- A producer transforming raw Fivetran data into tested, documented marts.
- **Enforced contracts** + Unity Catalog **PK/FK** that stop breaking changes.
- A **Mesh**: two consumers building on governed public models via cross-project refs.
- A **materialized view**, **unit tests**, and a **semantic layer**.
- One **lineage graph** spanning sources → three projects → a dashboard exposure.

---

## Appendix A — local CLI path (no dbt platform)

Labs 1, 2, 4, 5, 6 run against the single `platform` project locally with the
Fusion CLI. Lab 3 (cross-project Mesh) requires the dbt platform because
cross-project refs resolve through its publication artifacts.

```bash
cd platform
cp profiles.yml.example profiles.yml      # then edit, or place in ~/.dbt/
export DBT_DATABRICKS_HOST="adb-xxxx.azuredatabricks.net"
export DBT_DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxx"
export DBT_DATABRICKS_TOKEN="dapiXXXX"

dbt deps
dbt parse --profiles-dir .                 # static validation, no warehouse needed
dbt build --profiles-dir .
dbt test --select test_type:unit --profiles-dir .
```

`dbt parse` is a fast way to validate project structure and contracts without
touching the warehouse.

---

## Appendix B — troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `Failed to download publication artifact … 404` (consumer) | Producer `platform` not deployed. Run a production job in `platform` first (Lab 3a). |
| `dbt debug` connection fails | Check warehouse hostname/HTTP path/token and that the warehouse is running. |
| Contract error naming a column/type | A model's output doesn't match its contract. Fix the `cast(...)` in the model (Lab 2a) — this is the contract working. |
| Source `database`/`schema` not found | `raw_catalog`/`raw_schema` vars don't match your Fivetran destination. Update `platform/dbt_project.yml` or pass `--vars`. |
| Reference to a `platform` model rejected | That model is `protected`. Only the four public models are consumable across projects. |
| `ordered_products` explode errors | The nested payload's shape differs from the assumed `array<struct>`. See the TODO in `int_sales__order_items.sql` for the JSON-string variant. |

## Appendix C — reset

Re-running `dbt build` is idempotent. To start fully clean, drop your dev schema
in Databricks and rebuild:

```sql
DROP SCHEMA IF EXISTS main.<your_dev_schema> CASCADE;
```
```bash
cd platform && dbt build
```
