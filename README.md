# SDP Patterns Demo: Subscription Pipeline

**Date:** 2026-10-12 (updated 2026-10-26)

A working example for the *Databricks for Startups* post on choosing SDP patterns layer by layer: one subscription change feed, processed bronze to gold in SDP SQL, deployed with a Declarative Automation Bundle (DAB). You only need to set `catalog`, and optionally `schema`.

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

## Run it from the workspace

This is the path the post follows. It needs serverless compute and workspace files enabled.

1. **Clone the repo.** In your workspace, go to your home folder, click **Create > Git folder**, and paste the URL of this repository.
2. **Open the bundle.** Open the cloned folder and click **Open in editor**, next to the bundle name.
3. **Set the catalog.** In `databricks.yml`, add a default value for `catalog`, which has none, and change `schema` if you don't want the default `sdp_demo`. The catalog must already exist; the job creates the schema and the `raw` volume.

   ```yaml
   variables:
     catalog:
       description: Catalog for the demo volume and tables
       default: <your_catalog>
   ```

4. **Deploy.** Click the deployments icon (the rocket), then **Deploy**. When it finishes, the job and the pipeline appear under **Bundle resources**.
5. **Land the first batch.** Run `generate_subscriptions` with its play icon. It writes 10,000 events by default (the `num_records` job parameter, 1 to 1,000,000). Without the DAB, you can run the `src/generate_subscriptions` notebook directly and fill in `catalog`, `schema`, and `num_records`.
6. **Run the pipeline.** Run `sdp_patterns_demo` from **Bundle resources** the same way.
7. **Give the Replace Where table some history.** Its first run only fills the last 7 days. Change `-7` to `-90` in `src/transformations/gold_daily_mrr_rw.sql`, run the pipeline once, and set it back to `-7`. Workspace deployments to the `dev` target are source-linked, so the edit takes effect without redeploying. In production, use a one-time [predicate override](https://docs.databricks.com/aws/en/ldp/flows-replace-where#backfill-historical-data) instead of editing the definition.
8. **Land a second batch and compare.** Run `generate_subscriptions` again, on the same day (the generator computes start dates relative to the day it runs), then run the pipeline again. Compare `gold_daily_mrr_mv` with `gold_daily_mrr_rw`, and the per-table stats in the pipeline run view.

## Run it with the Databricks CLI

The bundle doesn't pin a workspace, so the CLI uses your default profile; add `--profile <name>` to any command to use another one. Set the catalog (required) and the schema (defaults to `sdp_demo`) in your shell, then deploy:

```bash
export BUNDLE_VAR_catalog=<catalog> BUNDLE_VAR_schema=<schema>
databricks bundle deploy
```

1. Land a batch of data: `databricks bundle run generate_subscriptions --params num_records=10000` (1 to 1,000,000). It creates the schema and the `raw` volume if they don't exist.
2. Run the pipeline: `databricks bundle run sdp_patterns_demo`.
3. Give the Replace Where table some history: change `-7` to `-90` in `src/transformations/gold_daily_mrr_rw.sql`, then run `databricks bundle deploy` and `databricks bundle run sdp_patterns_demo`. Set it back to `-7` and deploy again. CLI deployments copy the files, so each edit needs a deploy.
4. Land a second batch on the same day, run the pipeline again, and compare `gold_daily_mrr_mv` with `gold_daily_mrr_rw`.

## Data

8 columns: `subscription_id`, `customer_id`, `plan`, `status`, `monthly_price`, `country`, `start_date`, `updated_at`. Each record is a change event; about 4 events per subscription, start dates over the last 90 days. Each batch has about 2% exact duplicates and about 3% invalid rows (missing id, unknown plan, negative price) that the silver expectations drop.
