# SDP Patterns Demo: Subscription Pipeline

**Date:** 2026-10-12

A working example for the *Databricks for Startups* post on SDP patterns: one subscription change feed, processed bronze to gold in SDP SQL, deployed with a Declarative Automation Bundle. You only need to set `catalog` and `schema`.

## Layout

| Path | What it is |
|---|---|
| `databricks.yml` | Bundle, with `catalog` and `schema` variables |
| `resources/sdp_patterns_demo.pipeline.yml` | Serverless, triggered pipeline |
| `resources/generate_subscriptions.job.yml` | Job that runs the generator notebook |
| `src/generate_subscriptions.py` | Notebook that lands one batch of JSON events in `/Volumes/<catalog>/<schema>/raw/subscriptions` |
| `src/transformations/bronze_subscriptions.sql` | Append (Auto Loader), protected from full refresh |
| `src/transformations/silver_subscriptions.sql` | Auto CDC, SCD Type 1, with expectations |
| `src/transformations/gold_daily_mrr_mv.sql` | Gold as a materialized view |
| `src/transformations/gold_daily_mrr_rw.sql` | Gold as a Replace Where flow (last 7 days) |

## Run it

The bundle doesn't pin a workspace, so the CLI uses your default profile; add `--profile <name>` to any command to use another one. Set the catalog (required) and the schema (defaults to `sdp_demo`) in your shell, then deploy:

```bash
export BUNDLE_VAR_catalog=<catalog> BUNDLE_VAR_schema=<schema>
databricks bundle deploy
```

1. Land a batch of data: `databricks bundle run generate_subscriptions --params num_records=10000` (1 to 1,000,000). It creates the schema and the `raw` volume if they don't exist. Without the DAB, run the `src/generate_subscriptions` notebook directly and set `catalog`, `schema` and `num_records`.
2. Run the pipeline: `databricks bundle run sdp_patterns_demo`.
3. Land a second batch (updates to existing subscriptions), run the pipeline again, and compare `gold_daily_mrr_mv` with `gold_daily_mrr_rw`.

## Data

8 columns: `subscription_id`, `customer_id`, `plan`, `status`, `monthly_price`, `country`, `start_date`, `updated_at`. Each record is a change event; about 4 events per subscription, start dates over the last 90 days. Each batch has about 2% exact duplicates and about 3% invalid rows (missing id, unknown plan, negative price) that the silver expectations drop.
