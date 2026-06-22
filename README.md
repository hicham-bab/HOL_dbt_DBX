# HOL_dbt_DBX — dbt on Databricks hands-on lab

A production-style **dbt Mesh** for the *Fivetran + dbt on Databricks* hands-on
lab. Raw retail data is landed by Fivetran into Databricks Unity Catalog. One
**producer** project governs and publishes contracted interface models; two
**consumer** domains build their own analytics on top using cross-project
references — the dbt pattern for scaling analytics across teams on a lakehouse.

> **The full step-by-step guide is below** (Modules 1–6). It's SQL-only and
> written to take you from raw Fivetran tables to a governed Mesh, a semantic
> layer, and governed AI consumption.

## Contents

- [The three projects](#the-three-projects)
- [Public interface (the Mesh contract)](#public-interface-the-mesh-contract)
- [Topology](#topology)
- [What this lab demonstrates](#what-this-lab-demonstrates-dbt-alongside-databricks-native-tooling)
- **Step-by-step lab:**
  - [dbt vocabulary for Databricks people](#dbt-vocabulary-for-databricks-people)
  - [Prerequisites](#prerequisites)
  - [Module 1 — Ingest the raw data with Fivetran](#module-1--ingest-the-raw-data-with-fivetran)
  - [Module 2 — Connect dbt platform and explore the producer project](#module-2--connect-dbt-platform-and-explore-the-producer-project)
  - [Module 3 — What dbt adds that notebooks don't](#module-3--what-dbt-adds-that-notebooks-dont)
  - [Module 4 — Production and dbt State](#module-4--production-and-dbt-state)
  - [Module 5 — dbt Mesh (hands-on)](#module-5--dbt-mesh-one-platform-governed-cross-team-interfaces-hands-on)
  - [Module 6 — Semantic Layer + dbt MCP + Genie](#module-6--semantic-layer--dbt-mcp--genie-governed-ai-consumption)
- [Objection handling](#objection-handling-our-solution-isnt-technical-dbt-adds-engineering)
- [dbt vs DBX-native cheat sheet](#dbt-vs-dbx-native-cheat-sheet)
- [Appendices: MCP setup, local CLI, troubleshooting, reset](#appendix-a--connect-the-dbt-mcp-server-to-databricks-uc-http-connection)

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

---

## dbt vocabulary for Databricks people

Same concepts you already know, different words:

| dbt term | What it is | Databricks equivalent |
|----------|-----------|----------------------|
| **model** | A `.sql` file with a `SELECT`. dbt wraps it in `CREATE TABLE/VIEW` and derives run order from `ref()`. | Notebook cell / DLT `@dlt.table` |
| **`ref('model')`** | How models point at each other. dbt builds the DAG automatically — no task wiring. | Hard-coded table name + manual task dependency |
| **`ref('project','model')`** | Cross-project reference — how a consumer points at a producer's *public* model. | Sharing a table by name across workflows (no contract) |
| **materialization** | How a model is stored: `view`, `table`, `incremental`, `materialized_view`. | Delta table vs view vs DLT live table |
| **data contract** | Enforced promise about column names, types, and keys. Fails the build if broken — before the table is touched. | DLT expectations govern *data*; contracts govern *schema shape* |
| **test** | `not_null`, `unique`, `relationships`, `accepted_values` in YAML, or **unit tests** against mocked inputs. | DLT expectations / manual assertions |
| **snapshot** | SCD Type 2 history from one config block — dbt manages `dbt_valid_from`/`dbt_valid_to`. | `AUTO CDC` or hand-written `MERGE` |
| **Mesh** | Multiple dbt projects referencing each other's *public* models — governed cross-team interfaces. | Separate workflows sharing table names (no contract, no lineage) |
| **semantic layer** | Metrics (e.g. `total_revenue`) defined once, queried everywhere via MetricFlow. | Metric Views in Unity Catalog |
| **exposure** | A documented downstream consumer (e.g. a BI dashboard) surfaced in the lineage graph. | No direct equivalent |

> Throughout, **dbt vs native** callouts highlight what dbt adds over building
> the same thing with notebooks or DLT alone.

---

## Prerequisites

1. **Databricks**: a workspace with **Unity Catalog** and a **SQL warehouse**.
   Note your catalog name (the labs assume `main`). One warehouse is shared by
   the whole room.
   - For the **materialized view** in Module 5, the workspace must have a
     **serverless SQL warehouse** (Databricks materialized views and streaming
     tables run on serverless + Unity Catalog).
2. **A dbt platform account** (dbt Studio) with permission to create projects and
   connect to Databricks. The **Fusion engine** is enabled on the environment.
   - **dbt State** (Module 4) and **dbt Mesh** (Module 5) require **dbt
     Enterprise / Enterprise+** on a Fusion environment. If your account is
     Starter or single-project, run those modules presenter-led.
3. This repository — `HOL_dbt_DBX` — accessible from your dbt platform account
   (GitHub/GitLab/Azure DevOps).

> No dbt platform access? Modules 2, 3, and 6 run locally against `platform`
> with the Fusion CLI — see [Appendix B](#appendix-b--local-cli-path-no-dbt-platform). The Mesh
> module (5) requires the dbt platform.

---

## Module 1 — Ingest the raw data with Fivetran

**Goal:** land the six raw retail tables into a personal Unity Catalog schema.

1. In Fivetran, configure (or confirm) a connector that lands the retail source
   into a Databricks Unity Catalog destination schema. Each attendee uses a
   personal schema — e.g. `<yourfirstname>_<yourlastname>_retail`.
2. Confirm the six tables landed, each carrying Fivetran's `_fivetran_synced`
   (load timestamp) and `_fivetran_deleted` (soft-delete flag):
   `customers`, `loyalty_segments`, `ret_customers`, `ret_orders`,
   `ret_tickets`, `sales_orders`.
3. Note your **catalog** and **schema** names — you'll plug them into the
   producer project in the next module.

> **dbt vs native:** Fivetran lands raw data; dbt does every transformation from
> here as SQL pushed down to your Databricks warehouse. For SaaS sources,
> Fivetran's prebuilt **dbt packages** (HubSpot: 147 models, Salesforce,
> NetSuite…) drop in dozens of tested models with one line in `packages.yml` —
> the Fivetran + dbt compounding effect (Module 3, step 8).

---

## Module 2 — Connect dbt platform and explore the producer project

**Goal:** stand up the `platform` producer project, connected to Databricks, and
walk its structure.

1. **Connect dbt platform to Databricks.** In dbt Studio, create a Databricks
   connection with these fields (from your lab credentials page):

   | Field | Value | Example |
   |-------|-------|---------|
   | Server Hostname | your workspace host | `dbc-a2c61234-1234.cloud.databricks.com` |
   | HTTP Path | your **SQL warehouse** HTTP path | `/sql/1.0/warehouses/1a23b4596cd7e8fg` |
   | Catalog | your HOL Unity Catalog | `main` |
   | Auth | personal access token | `dapi…` |

   Then set your **development credentials**: a personal dev **schema** —
   e.g. `dbt_<initials>` — so your builds are isolated. Zero infra, instant
   environment; one SQL warehouse shared by the room.
2. **Create the `platform` project** in dbt Studio: point it at the
   `HOL_dbt_DBX` repo with **project subdirectory = `platform`**. This is the
   *producer* — it owns the Fivetran source tables and publishes four governed
   models the other two projects build on. Set the raw-data vars to match your
   Fivetran destination schema from Module 1.

   In `platform/dbt_project.yml`, find the `vars:` block and set:
   ```yaml
   vars:
     raw_catalog: main                                   # your Unity Catalog
     raw_schema: <yourfirstname>_<yourlastname>_retail   # your Fivetran destination schema
   ```
   Use lowercase and underscores only — match exactly what you set as the
   Fivetran destination schema prefix in Module 1. Save the file.

   > **👥 Sharing one dbt account?** Project **display names** must be unique in
   > an account, so a roomful of people each creating `platform` will collide.
   > Two options:
   > - **Recommended for this lab:** one person creates the three projects
   >   (`platform`, `marketing`, `finance`); everyone else opens them and develops
   >   on their **own git branch**. No duplicate projects, and only one producer
   >   needs deploying for cross-project refs.
   > - **Per-person projects:** give the *display name* a personal suffix
   >   (e.g. `platform — jdoe`) but **leave the internal project `name:` in
   >   `dbt_project.yml` as `platform`** — cross-project `ref('platform', …)` and
   >   `dependencies.yml` key off that internal name, not the display name.

   > **⚠️ Having trouble?** If your Fivetran sync isn't finished (or you hit
   > errors you can't resolve), set `raw_schema` to the shared instructor schema
   > `hicham_babahmed_retail` to use pre-loaded source data and continue the lab
   > without interruption.
3. **Verify the raw data in Databricks** (while the connection is fresh). In the
   Databricks **Catalog** explorer, open your catalog → your
   `<yourfirstname>_<yourlastname>_retail` schema → **Tables** → `sales_orders`
   → **Sample Data**. Scroll right to the `_fivetran_synced` column.

   > ✅ **Expected:** rows of e-commerce orders loaded by Fivetran, each with a
   > `_fivetran_synced` timestamp — the marker Fivetran adds to every table it
   > manages.
4. **Walk the project structure** in dbt Studio (file tree on the left):
   - `models/staging/retail/` — **Bronze → Silver views**: rename columns, cast
     types, drop soft-deleted rows. One staging model per Fivetran source table
     (`stg_retail__sales_orders.sql`, `stg_retail__customers.sql`,
     `stg_retail__ret_orders.sql`, …).
   - `models/intermediate/` — **ephemeral CTEs** (never materialized as a
     warehouse table, inlined into downstream SQL): `int_sales__order_items`
     explodes the nested `ordered_products` payload into one row per line item;
     `int_customers__enriched` deduplicates customers and joins loyalty data.
   - `models/marts/` — **Gold Delta tables**: `dim_customers`, `fct_sales`
     (incremental merge), `fct_orders`, `dim_loyalty_segments`,
     `fct_support_tickets`.
   - `snapshots/customers_snapshot.sql` — full SCD Type 2 history of customer
     loyalty-segment and address changes, declared in one config block.
   - `semantic_models/sem_retail.yml` — `total_revenue`, `total_units`,
     `avg_basket_value`, `revenue_per_customer`, defined once for everyone.
5. **Run `dbt build`** in the IDE. Then switch to Databricks **Query History**:
   every model compiled to SQL and pushed down to the SQL warehouse. This is the
   consumption argument live — no compute anywhere except Databricks.

   > ✅ **Expected:** every model finishes with a green `OK` / `CREATE TABLE` /
   > `CREATE VIEW` in the run logs, and the compiled SQL paths reference *your*
   > dev schema. If you still see `hicham_babahmed_retail` everywhere, confirm
   > you saved `dbt_project.yml` and re-run.
6. **Open `stg_retail__sales_orders.sql`** — it's a plain `SELECT`: renamed
   columns, type casts, one `where not coalesce(_fivetran_deleted, false)`. dbt
   inferred the run order, wrote the `CREATE VIEW` DDL, and assigned the correct
   schema. You wrote only the transformation logic.

> **dbt vs native:** you declared *where* compute and data live once; every model
> inherits it — no per-notebook connection setup.

---

## Module 3 — What dbt adds that notebooks don't

Hands-on — attendees do each step.

### 3.1 Tests (data quality)
Run `dbt test`. Generic tests — `unique`, `not_null`, `accepted_values`,
`relationships`, `dbt_utils.accepted_range` — are declared in ~4 lines of YAML
each (see `platform/models/staging/retail/_retail__models.yml` and
`models/marts/_marts__models.yml`).

### 3.2 Unit tests (transformation logic) — break one, watch it fail
`platform/models/intermediate/_int__unit_tests.yml` tests the trickiest logic in
the project against mocked inputs (no warehouse data):

```bash
dbt test --select test_type:unit
```
Now break it on purpose. In `platform/models/intermediate/int_sales__order_items.sql`
change:
```sql
unit_price * quantity as line_revenue
```
to
```sql
unit_price + quantity as line_revenue   -- wrong on purpose
```
Re-run `dbt test --select test_type:unit` → the explode test fails with
expected-vs-actual `line_revenue`. Revert it.

> **dbt vs native:** unit tests catch logic regressions (a bad join, a wrong
> formula) on every change. DLT expectations validate the *data*; unit tests
> validate the *transformation*.

### 3.3 Data contracts — break one, watch the build stop
The four public models declare **enforced contracts** (column names, types,
PK/FK). In `platform/models/marts/fct_sales.sql` change:
```sql
cast(quantity as bigint) as quantity,
```
to:
```sql
cast(quantity as string) as quantity,
```
Run `dbt build --select fct_sales` → the build **fails before writing the
table**: the contract says `quantity` is `bigint`. Revert and re-run.

> **dbt vs native:** the contract caught a breaking schema change *before* it
> reached the table `marketing` and `finance` depend on — the guardrail that
> makes cross-team consumption safe.

### 3.4 Source freshness
```bash
dbt source freshness
```
SLA monitoring on Fivetran loads (via `_fivetran_synced`), declared in YAML under
the source `config:` block.

### 3.5 Incremental models
Open `platform/models/marts/fct_sales.sql` — declarative `is_incremental()` +
Delta `merge`. Run `dbt build` twice: the second run only merges orders newer
than what's already loaded. No manual `MERGE INTO`, no checkpoint bookkeeping.

### 3.6 Snapshots
```bash
dbt snapshot
```
`customers_snapshot` captures SCD Type 2 history of customer loyalty-segment and
address changes — from one config block (`check` strategy, `check_cols`).

### 3.7 Docs and column-level lineage
Run `dbt docs generate` and open the dbt **Catalog**: column-level lineage from
Fivetran raw table → mart → dashboard exposure, auto-generated. In Unity Catalog,
open `dim_customers` and note the **table and column comments** dbt pushed there
via `persist_docs`.

### 3.8 Packages
The projects already use `dbt_utils`. Show the dbt package hub and Fivetran's
prebuilt packages (HubSpot: 147 models, Salesforce, NetSuite…) — for SaaS
sources, `dbt deps` drops in dozens of tested models. The Fivetran + dbt
compounding effect.

### 3.9 dbt Wizard — governed AI development
dbt's AI agent, built into dbt Studio. Unlike a general coding assistant, it has
full access to the project's **native metadata engine** — lineage, tests,
contracts, run results, semantic definitions — so it understands the full
context before touching code. It shows a file diff and waits for your approval
before saving. In preview since May 2026 on Starter, Enterprise, and
Enterprise+ plans.

**What makes it different from GitHub Copilot or ChatGPT:**
- It reads `ref()` dependencies — it knows which models *downstream* of your
  change will break.
- It reads enforced contracts — it won't suggest a column rename that would
  silently break `marketing` or `finance`.
- It reads run results — it can tell you if a model last failed in production and
  show the error.
- It never overwrites silently — it shows a diff, you approve.

**Hands-on prompts — all based on the actual `HOL_dbt_DBX` models:**

🔍 **Understand** (a great starting point):
- *"Explain what `fct_sales` does in the `platform` project. What upstream models
  does it depend on, which consumer projects reference it, and what tests are
  defined on it?"*
- *"Walk me through the lineage from the Fivetran source tables to
  `mart_customer_loyalty` in the `marketing` project — what transformation
  happens at each step?"*

🏗️ **Create** (watch it check upstream/downstream impact before generating):
- *"In the `platform` project, create a model `mart_customer_rfm` that scores
  every customer in `dim_customers` by recency, frequency, and monetary value
  using `fct_sales`. Add `not_null` and `unique` tests on `customer_business_key`,
  write a description for every column, and use only models that already exist in
  this project."*
- *"In the `marketing` project, create a model `mart_loyalty_cohort_analysis`
  that groups customers from `dim_customers` by their first purchase month
  (derived from `fct_sales`) and tracks their cumulative revenue over the
  following 6 months. Add generic tests and column descriptions."*

✂️ **Refactor** (the impact-awareness demo — where Wizard shines):
- *"Refactor `mart_customer_loyalty` in the `marketing` project: extract the
  revenue-per-customer aggregation into a new intermediate model
  `int_customers__revenue_aggregated`, then have `mart_customer_loyalty`
  reference it. Show which downstream models are affected, and update tests and
  docs accordingly."*

🔬 **Extend and debug:**
- *"Extend `mart_b2b_orders` in the `finance` project to add a `gross_margin_pct`
  column. Add a dbt_utils `accepted_range` test that warns if any value falls
  below 0% or above 100%."*
- *"`fct_daily_revenue` in `finance` is a materialized view built on `fct_sales`
  from `platform`. If I change the data type of `line_revenue` in `fct_sales`,
  what breaks — and does the data contract protect against this change reaching
  production?"*

📐 **Semantic layer:**
- *"Add a metric `avg_order_value` to `sem_retail.yml` in the `platform` project,
  defined as `total_revenue` divided by a count of distinct orders on the
  `fct_sales` semantic model. Add `customer__region` and `order_datetime` as
  available dimensions."*

**Talking point:** every prompt above would require multiple back-and-forths with
a generic coding assistant plus manual context-pasting. Wizard already knows the
schema, contracts, and lineage — it gets to the right answer in one shot.

**BYOK — the customer-choice story:** dbt Wizard connects to **whatever AI model
the customer already uses** — OpenAI (managed or BYOK), Anthropic, Azure AI
Foundry/Azure OpenAI, and in the CLI also AWS Bedrock and Databricks Genie. No
forced model; keys and data governance stay with the customer — a strong answer
for security-conscious EMEA enterprises
(docs: https://docs.getdbt.com/docs/dbt-ai/wizard-ide).

### 3.10 dbt State (Preview) — never rebuild what hasn't changed
dbt State makes every `dbt build` state-aware: before running a node it checks
whether the logic changed AND whether upstream data is actually new. If not, it
**skips** the node or **clones** it from another environment at a fraction of the
compute, and **auto-defers** to production with no `--defer`/`--state` flags or
manifest juggling. Unlike `state:modified`, it understands SQL semantically —
whitespace or alias changes don't trigger rebuilds — and it checks source
freshness, so an unchanged model with no new upstream data simply doesn't run.

Hands-on:
- Run `dbt build` twice — second run: nodes reused/skipped, near-zero warehouse
  time.
- Add whitespace to a model and build again — the semantic diff says nothing
  changed.
- In a fresh dev schema, build one mart — watch upstream tables get **cloned**
  from prod instead of rebuilt.

> SA framing, honest: dbt State removes *wasted* consumption, not consumption —
> customers redeploy that budget into net-new workloads, and the efficient
> platform is the one that grows. Works with dbt Core, the dbt platform, and the
> Fusion engine (docs: https://docs.getdbt.com/docs/deploy/dbt-state-about).

---

## Module 4 — Production and dbt State

### 4.1 Create a production job
1. In the `platform` project, go to **Orchestration → Environments** and create a
   **Production environment** (target `prod`, schema e.g. `analytics`) on the
   **Fusion** engine.
2. Go to **Orchestration → Jobs → Create job → Deploy job**. In **Execution
   settings**, confirm the command is `dbt build`, add a second command
   `dbt source freshness`, and check **Generate docs on run**. Save.
3. Click **Run now** and watch the run logs, then explore run history and
   alerting.

   > ✅ **Expected:** all models build with green checkmarks; docs are generated.
   > This run also publishes the producer's artifact other projects consume
   > (Module 5).

### 4.2 dbt State — never rebuild what hasn't changed
Make the production job state-aware so it only builds what actually changed.

1. Open the Prod job → **Settings → Edit**.
2. Turn on **Enable Fusion cost optimization features**. This enables
   **State-aware orchestration**; expand **More options** and also enable
   **Efficient testing** (it's off by default) so tests on skipped models are
   skipped too.
3. Under **Advanced settings**, set **Compare changes against → Environment** and
   select your **Production** environment. Save.
4. **Run #1 (baseline).** Click **Run now**. With no prior state to compare
   against, dbt builds everything and records the current state.

   > ✅ **Expected:** all models execute (`CREATE`/`OK`). Note the run time.
5. **Run #2 (nothing changed).** No new Fivetran data has loaded, so click **Run
   now** again.

   > ✅ **Expected:** every model is tagged **Reused/SKIP**, tests are skipped,
   > and the job finishes in seconds with near-zero warehouse compute. Compare to
   > Run #1.

> **dbt vs native:** on a schedule (hourly/daily) you pay warehouse compute only
> when data actually changed. For hundreds of models that's a large cut in both
> cost and runtime — *wasted* consumption removed, not consumption.
> (Docs: https://docs.getdbt.com/docs/deploy/state-aware-setup.)

### 4.3 Orchestrate from Databricks + close the loop
1. Mention the **dbt platform task type in Databricks Jobs** — orchestrate dbt
   platform jobs natively from Databricks workflows.
2. Build a quick **AI/BI dashboard** on a mart — `mart_customer_loyalty` (revenue,
   basket value and units by loyalty segment and region) or `mart_b2b_orders`
   (gross vs net booked amount by status). It's registered as an **exposure** in
   dbt (`marketing/models/_marketing__exposures.yml`), so lineage reaches the
   dashboard.
3. Recap the SA value: **consumption, coverage, speed, governance.**

### 4.4 CI job — test every pull request before it merges
A **CI job** runs automatically when someone opens a PR, builds only the
*modified* models + their downstream, and reports pass/fail back on the PR. This
is the engineering discipline (PR review + automated checks) that no-code
pipelines don't have.

1. Create a deployment environment for CI (dbt Labs recommends a dedicated
   **CI/staging** environment; a production environment also works for the lab).
2. **Orchestration → Jobs → Create job → Continuous integration job.**
3. **Git trigger:** leave **Triggered by pull requests** on (optionally enable
   **Run on draft pull request**).
4. **Execution settings → Commands** (defaults shown — keep them):
   ```bash
   dbt build --select state:modified+        # build only changed models + downstream
   dbt sl validate --select state:modified+  # validate metrics affected by the change
   ```
5. **Compare changes against an environment (Deferral):** set to **Production**.
   State comparison (`state:modified+`) needs a deferred environment to diff
   against — this is what limits the run to just your changes. Save.
6. **See it work.** On a branch, reintroduce the **contract break** from Module
   3.3 (`fct_sales.quantity` → `string`) and open a PR.

   > ✅ **Expected:** the CI check goes red on the PR — the contract violation is
   > caught in review, before it can reach `main` or any consumer. Fix and the
   > check goes green. (Optional: with **Advanced CI** enabled, turn on **dbt
   > compare** — our PK/uniqueness constraints let it produce a row-level diff.)

> **⚠️ Monorepo note:** this repo holds three dbt projects, so a PR can trigger
> CI for all connected projects. If that's noisy, give each project a separate
> target branch (e.g. `main-platform`, `main-marketing`) in its environment's
> custom-branch settings. (Docs: https://docs.getdbt.com/docs/deploy/ci-jobs.)

### 4.5 Merge job — continuous deployment on merge
A **merge job** runs when a PR merges, so production data updates automatically
after review — continuous deployment.

1. **Orchestration → Jobs → Create job → Merge job** (in your Production
   environment).
2. **Git trigger:** **Run on merge** is on by default — it fires whenever a PR
   merges into the environment's base branch.
3. **Commands:** keep the default `dbt build --select state:modified+` so only the
   newly merged changes (and downstream) rebuild. Save.

   > ✅ **Expected:** merging the fix from 4.4 triggers the merge job; only the
   > changed models rebuild in production. (A merge job also refreshes the
   > environment's `manifest.json`, keeping the CI job's deferral state fresh.)
   > (Docs: https://docs.getdbt.com/docs/deploy/merge-jobs.)

---

## Module 5 — dbt Mesh: one platform, governed cross-team interfaces (hands-on)

The three projects aren't just folders — they're independent dbt projects, and
that's the dbt Mesh story: domain teams owning their own projects while sharing
governed, contracted interfaces. *(Requires dbt Enterprise. If attendee accounts
are single-project, run this presenter-led against the deployed projects.)*

1. **The setup.** `platform` is the upstream producer domain; `marketing` and
   `finance` are downstream consumer domains that depend on it.
2. **Public models + contracts.** In `platform`, four models carry
   `access: public`, enforced **contracts** (column names + types), Unity Catalog
   **PK/FK**, and the `retail` `group` with a named owner:
   - `dim_customers` (PK `customer_business_key`)
   - `fct_sales` (PK `sales_order_line_id`, FK → `dim_customers`)
   - `fct_orders` (PK `order_id`)
   - `dim_loyalty_segments` (PK `loyalty_segment_id`)

   Everything else (e.g. `fct_support_tickets`) stays `protected` — implementation
   details are not an interface.
3. **Deploy the producer.** Cross-project refs resolve against the producer's
   **production publication artifact**. In the `platform` project, create a
   `prod` environment and run a `dbt build` job once.

   > If a consumer build errors with *"Failed to download publication artifact …
   > 404"*, the producer hasn't been deployed yet — do this step first.
4. **Cross-project ref.** Each consumer declares the producer in
   `dependencies.yml`:
   ```yaml
   projects:
     - name: platform
   ```
   then builds on governed models, e.g. `marketing/models/mart_customer_loyalty.sql`:
   ```sql
   select * from {{ ref('platform', 'dim_customers') }}
   select * from {{ ref('platform', 'fct_sales') }}
   ```
   Create the `marketing` and `finance` projects in dbt Studio (subdirectories
   `marketing`, `finance`), then `dbt deps && dbt build`. No source duplication,
   no stale copies of someone else's tables.

   > ✅ **Expected:** the consumer build succeeds and resolves
   > `ref('platform', …)`. If you see *"Failed to download publication artifact …
   > 404"*, the producer hasn't run a production job yet — complete step 3 first.
5. **Materialized view (finance) — the DLT contrast.**
   `finance/models/fct_daily_revenue.sql` is an ordinary dbt model with
   `materialized='materialized_view'`, built on `ref('platform', 'fct_sales')`.
   `dbt build` issues `CREATE MATERIALIZED VIEW`; dbt manages the definition and
   refresh and keeps it in the lineage graph. Verify in Databricks:
   ```sql
   DESCRIBE EXTENDED main.<finance_schema>.fct_daily_revenue;
   ```
   > **Requires a serverless SQL warehouse + Unity Catalog** (Databricks MVs run
   > on serverless). **dbt vs native:** same model file you'd write for a table —
   > only the `materialized` config changes — versioned and code-reviewed, vs a
   > separate DLT pipeline with its own framework and lineage.
6. **Governance teeth (live demo).**
   - Reference a **protected** producer model from a consumer — e.g.
     `{{ ref('platform', 'fct_support_tickets') }}` — and parse:
     `DbtReferenceError`. Only the four public models cross the boundary.
   - Make a contract-breaking column change on `fct_sales` (Module 3.3) → blocked
     in CI before it ships. In a notebook estate, nothing stops one team from
     querying another team's intermediate tables.
7. **Cross-project lineage** in dbt Catalog/Explorer: Fivetran source → `platform`
   gold → `marketing`/`finance` marts → dashboard exposure, across project
   boundaries.

> **Why the SA cares:** Mesh is how large EMEA enterprises scale dbt beyond one
> team — more domains, more SQL warehouses, more governed consumption on
> Databricks. Unity Catalog governs the data; dbt Mesh governs the
> transformations and the interfaces between teams. (Docs:
> https://docs.getdbt.com/docs/mesh/about-mesh.)

---

## Module 6 — Semantic Layer + dbt MCP + Genie: governed AI consumption

The payoff module — the metrics layer makes the whole stack AI-ready.

1. **Define metrics once.** Open `platform/semantic_models/sem_retail.yml` —
   semantic models over `fct_sales` and `dim_customers`, with four metrics:
   - `total_revenue` — sum of `line_revenue` from `fct_sales`
   - `total_units` — sum of `quantity`
   - `avg_basket_value` — revenue per order
   - `revenue_per_customer` — revenue divided by distinct customer count

   The definition lives in version control, next to the models that feed it,
   tested and code-reviewed. No two dashboards can disagree on "revenue" when
   there's one definition.
2. **Enable the Semantic Layer** (one-time, per project). In **Account Settings →
   [project] → Project Details**, click **Configure Semantic Layer**: enter the
   Databricks connection credentials for the Semantic Layer (a least-privileged
   user with `SELECT` + `CREATE TABLE` is recommended) and select the
   **Production environment**. The environment needs one successful run — `dbt
   build` from Module 4 satisfies this. Then **Generate a service token** and save
   it for downstream tools.
3. **Query through the Semantic Layer.** In dbt platform, ask for `total_revenue`
   grouped by `customer__region` by month — MetricFlow compiles the SQL and pushes
   it to the Databricks SQL warehouse. Add `loyalty_segment` as a dimension, or
   change the grain to week — never rewrite the SQL.
4. **Connect the dbt MCP server to an AI agent.** First ensure **AI features are
   enabled** for the account (**Account Settings → enable AI**). The remote MCP
   endpoint is `https://<your-dbt-host>/api/ai/v1/mcp/` and needs your
   **production environment ID** (find it under Orchestration). Two options: any
   MCP client (e.g. Claude — see [Appendix A](#part-d--alternative-connect-any-mcp-client-eg-claude)),
   or — the crowd-pleaser for this audience — **Databricks AI Playground via a
   Unity Catalog HTTP connection** (full setup in
   [Appendix A](#appendix-a--connect-the-dbt-mcp-server-to-databricks-uc-http-connection)).
   Live demo prompts:
   - *"What metrics do we have?"* → agent calls `list_metrics` — discovers
     `total_revenue`, `avg_basket_value`, `revenue_per_customer`, no schema
     spelunking.
   - *"What was total revenue by customer region last month?"* → agent calls
     `query_metrics` — THE governed number, computed through MetricFlow on
     Databricks. Compare with letting an LLM write raw SQL against bronze:
     plausible answer, confidently wrong number.
   - *"Where does `avg_basket_value` come from?"* → lineage tools trace metric →
     `fct_sales` → `int_sales__order_items` → Fivetran `sales_orders` source.
5. **Genie on the gold layer.** Create a Genie space on `mart_customer_loyalty` +
   `dim_customers` (or `mart_b2b_orders` + `fct_orders`). Because dbt built clean,
   documented, well-named Gold tables — and pushed column descriptions into Unity
   Catalog via `persist_docs` — Genie's answers get dramatically better. dbt is
   the data-quality foundation that makes Genie shine.
6. **The joint story for the SA:** Databricks provides the compute, governance, and
   Genie UX; dbt provides the trusted transformations and metric definitions; MCP
   makes both consumable by any agent. AI on the lakehouse is only as good as the
   data layer beneath it — and that layer is built with dbt, running on Databricks.

---

## Wrap-up — discussion

When do you position Lakeflow vs Fivetran + dbt? *(Honest answer: Lakeflow for
covered sources and Spark-centric teams; Fivetran + dbt for long-tail sources and
SQL-first analytics teams — often both in one account.)*

---

# Objection handling: "our solution isn't technical, dbt adds engineering"

The most common challenge from Databricks SAs: *"Lakeflow Designer is no-code,
Genie is natural language — why add dbt's engineering overhead?"* Counters, from
reframe to receipts:

## 1. Reframe: dbt is not "more engineering" — it's SQL
A dbt model is a SELECT statement. The DAG, dependency order, materialization,
incremental merge, docs, and lineage are all inferred or declared in YAML. No
Spark, no Python, no API to learn — this 2h lab takes **dbt beginners** from raw
Fivetran tables to a tested gold layer with a semantic layer on top. If the
audience can write SQL, they can use dbt — the same skills bar as Lakeflow
Designer's target user, with version control included.

## 2. "No-code" is cheap on day 1 and expensive on day 400
Ask: *how do you code-review a drag-and-drop pipeline? How do you diff it? Roll it
back? Who maintains it when its author leaves?* dbt's answer is boring and proven:
it's text in git — PRs, reviews, CI, rollback for free. No-code artifacts become
unreviewable black boxes; the "engineering" wasn't removed, it was deferred to
incident time. Even Databricks frames Designer as needing a *"bridge between
no-code and pro-code"* — that bridge is exactly what dbt already is.

## 3. The comparison is backwards: DBX-native needs MORE code for the same outcome

| Outcome | dbt | DBX-native |
| --- | --- | --- |
| Data quality test | 2–4 lines of YAML (`unique`, `not_null`, `relationships`) | Expectations/DQX written in code |
| SCD Type 2 history | One snapshot config block | AUTO CDC API or hand-written MERGE |
| Incremental load | `is_incremental()` • one config | MERGE logic, checkpoints |
| 147 HubSpot models | `dbt deps` — one line in packages.yml | Build them yourself |
| Docs + column lineage | Auto-generated from code | UC lineage exists, but no docs-as-code |
| Dev environment per person | A schema name | A cluster / workspace setup |

If "less engineering" is the criterion, dbt wins the comparison they started.

## 4. "Simple" without tests = simple until it's wrong
Genie and AI consumption raise the stakes: an agent answering from an untested,
undocumented table is confidently wrong at scale. The reason Module 6's Genie demo
works so well is that dbt built clean, tested, documented gold tables underneath
it. **The "no engineering" pitch and the AI pitch contradict each other** — AI on
the lakehouse is only as good as the discipline in the transformation layer.

## 5. Receipts: Databricks itself invests in dbt
Databricks maintains the dbt-databricks adapter, ships a native **dbt platform
task** in Lakeflow Jobs, and publishes joint Fivetran + dbt reference
architectures. Their own engineering best practices (Asset Bundles, CI/CD,
environments) are the same software practices dbt gives analytics teams out of the
box. dbt isn't a competing philosophy — it's their philosophy, made accessible to
SQL people.

## Quick rebuttal one-liners

| They say | You say |
| --- | --- |
| "Designer is no-code" | "dbt is SQL-only — same skills bar, plus git, tests and CI for free. How do you code-review a canvas?" |
| "dbt adds complexity" | "Four lines of YAML vs hand-written expectation code — which is complex?" |
| "We have Declarative Pipelines" | "Great for Spark teams. Where do your customer's 50 SQL analysts work? They're already in dbt — bring that consumption here." |
| "Genie means no modeling needed" | "Genie on raw bronze hallucinates. Genie on a dbt gold layer shines — we'll show both." |
| "Metric Views replace the semantic layer" | "Metric Views are warehouse-local. dbt metrics are version-controlled, tested, and exposed to any agent via MCP." |
| "This is just extra cost" | "Every dbt run is SQL warehouse consumption. dbt grows your workload, it doesn't tax it." |

*Tone tip for the room: agree that simplicity matters, then redefine it — "simple"
should mean simple to trust, maintain, and hand over, not just simple to click
together in a demo.*

---

# dbt vs DBX-native: cheat sheet

| Capability | dbt on Databricks | DBX-native equivalent |
| --- | --- | --- |
| Source connectors (via Fivetran) | 700+ | ~10 managed SaaS connectors in Lakeflow Connect (no Marketo) |
| Data tests | Declarative YAML, 4 lines | DQX / expectations — more code, less convention |
| Prebuilt transformation packages | Fivetran dbt packages, dbt_utils, 1000s on dbt package hub | None comparable |
| SCD2 history | Snapshots: one config block | AUTO CDC / hand-written MERGE |
| Docs + column-level lineage | Auto-generated, ties to exposures/dashboards | Unity Catalog lineage (table/column, but no docs-as-code) |
| Dev environments | Per-developer schema, zero infra | Per-user clusters/notebooks |
| CI/CD | Built into dbt platform (Slim CI, dbt State) | DABs — more setup, engineer-oriented |
| State-aware builds | dbt State: skip/clone/auto-defer, semantic SQL diff, source-freshness aware | Rebuild everything, or hand-rolled change detection |
| Multi-team / domain ownership | dbt Mesh: public models, contracts, cross-project ref + lineage | Separate workspaces; no contract semantics between teams |
| Audience | SQL-fluent analytics engineers | Spark/Python data engineers |

*Positioning note: this is "better together", not "instead of". Every dbt workload
is Databricks SQL consumption.*

---

# Appendix A — Connect the dbt MCP server to Databricks (UC HTTP connection)

This wires dbt's hosted MCP server into Databricks as a **Unity Catalog HTTP
connection**, so Databricks-native agents (AI Playground, Agent Bricks, Mosaic AI
agents) can call dbt MCP tools like `list_metrics` and `query_metrics` directly —
with per-user OAuth, governed on both sides. This is the strongest version of the
Module 6 demo: *Databricks' own agent stack consuming dbt's Semantic Layer.*

## Part A — dbt platform: register the OAuth app integration
1. **Settings → Integrations → App integrations → Add integration**.
2. **Integration name:** e.g. `dbx_mcp` (unique per account).
3. **Redirect URI:** your Databricks workspace OAuth callback —
   `https://<workspace-host>/login/oauth/callback`
   (e.g. `https://dbc-xxxxxxxx.cloud.databricks.com/login/oauth/callback`).
4. **Create integration** → copy the generated **client ID**. These integrations
   use PKCE — **no client secret is issued** (leave the secret field empty on the
   Databricks side).
5. From the same Integrations page, copy the account's **MCP Endpoint URL**:
   `https://<dbt-host>/api/ai/v1/mcp` (e.g. `https://da111.eu1.dbt.com/api/ai/v1/mcp`).

## Part B — Databricks: create the UC HTTP connection
**Catalog → External data → Connections → Create connection**

**Step 1 — Connection basics**

| Field | Value |
| --- | --- |
| Connection name | `dbt_mcp_<name>` |
| Connection type | HTTP |
| Catalog / Schema | where the connection object lives (e.g. `hol_catalog` / `default`) |
| Auth type | **OAuth User to Machine Per User** |
| OAuth provider | Manual configuration |

**Step 2 — Authentication**

| Field | Value |
| --- | --- |
| Host | `https://<dbt-host>` (e.g. `https://da111.eu1.dbt.com`) |
| Port | 443 |
| Client ID | from Part A step 4 |
| Client secret | *(leave empty — PKCE)* |
| Authorization endpoint | `https://<dbt-host>/oauth/authorize` |
| OAuth scope | `offline_access account:read projects:query catalog:read projects:develop jobs:run` |

*Scope tips: `offline_access` is required for refresh tokens. For a read-only demo
agent, drop `projects:develop` and `jobs:run` — a nice governance talking point:
you scope what the agent is allowed to do.*

**Step 3 — Connection details**

| Field | Value |
| --- | --- |
| **Is mcp connection** | ☑️ **must be checked** — this is what makes it selectable as an MCP server for agents |
| Base path | `/api/ai/v1/mcp` |
| OAuth credential exchange method | Header and body (default) |
| Token endpoint | `https://<dbt-host>/oauth/token` |

Save. Each user is prompted to sign in to dbt on first use (per-user OAuth — dbt
permissions are enforced per person, not via a shared service account).

## Part C — Use it from a Databricks agent
1. Open **AI Playground** (or Agent Bricks), add tools → **MCP server** → select
   the UC connection.
2. Demo prompts: *"What metrics are available?"* → `list_metrics`; *"Total revenue
   by customer region, last month"* → `query_metrics` — the governed number,
   computed through MetricFlow on the SQL warehouse.
3. Talking point: the agent lives in Databricks, the auth is Unity Catalog-governed,
   and the answer comes from dbt's metric definition — better together, end to end.

## Part D — alternative: connect any MCP client (e.g. Claude)

If you'd rather demo from a generic MCP client instead of Databricks, dbt hosts a
**remote MCP server** over HTTP — no local install. First enable **AI features**
(Account Settings → enable AI), then point your client at the endpoint.

- **MCP URL:** `https://<your-dbt-host>/api/ai/v1/mcp/`
- **Auth:** OAuth (browser sign-in on first connect) *or* token-based with
  headers `Authorization: Token <PAT-or-service-token>` and
  `x-dbt-prod-environment-id: <prod-env-id>`.

Claude Code example (`.mcp.json` at the project root, OAuth):
```json
{
  "mcpServers": {
    "dbt": { "type": "http", "url": "https://YOUR_DBT_HOST_URL/api/ai/v1/mcp/" }
  }
}
```
Then ask *"What metrics are defined in my Semantic Layer?"* — the client calls
`list_metrics`/`query_metrics` against the governed definitions.
(Docs: https://docs.getdbt.com/docs/dbt-ai/mcp-quickstart-remote.)

## MCP troubleshooting
- **Redirect/callback error during sign-in:** the Redirect URI in the dbt app
  integration must exactly match the workspace callback URL Databricks shows.
- **401 on tool calls:** check the scope string (space-separated, includes
  `offline_access`) and that the token endpoint is `/oauth/token` on the same dbt
  host as the authorize endpoint.
- **Connection saved but not offered as an MCP tool:** the *Is mcp connection*
  checkbox wasn't ticked (unchecked by default — easy to miss when editing).
- **Semantic Layer tools missing:** Semantic Layer must be enabled on the dbt
  account and the project must have metrics deployed in the environment.

---

# Appendix B — local CLI path (no dbt platform)

Modules 2, 3, and 6 run against the single `platform` project locally with the
Fusion CLI. The Mesh module (5) requires the dbt platform, because cross-project
refs resolve through its publication artifacts.

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

# Appendix C — lab troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `Failed to download publication artifact … 404` (consumer) | Producer `platform` not deployed. Run a production job in `platform` first (Module 5, step 3). |
| `dbt debug` connection fails | Check warehouse hostname/HTTP path/token and that the warehouse is running. |
| Contract error naming a column/type | A model's output doesn't match its contract. Fix the `cast(...)` in the model — this is the contract working (Module 3.3). |
| Source `database`/`schema` not found | `raw_catalog`/`raw_schema` vars don't match your Fivetran destination. Update `platform/dbt_project.yml` or pass `--vars`. |
| `DbtReferenceError` referencing a `platform` model | That model is `protected`. Only the four public models are consumable across projects (Module 5, step 6). |
| `ordered_products` explode errors | The nested payload's shape differs from the assumed `array<struct>`. See the TODO in `int_sales__order_items.sql` for the JSON-string variant. |

# Appendix D — reset

Re-running `dbt build` is idempotent. To start fully clean, drop your dev schema
in Databricks and rebuild:

```sql
DROP SCHEMA IF EXISTS main.<your_dev_schema> CASCADE;
```
```bash
cd platform && dbt build
```
