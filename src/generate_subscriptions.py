# Databricks notebook source
# MAGIC %md
# MAGIC # Generate sample subscription data
# MAGIC Lands one batch of subscription change events as JSON in `/Volumes/<catalog>/<schema>/raw/subscriptions`.
# MAGIC Run it again to land a new batch with updates to existing subscriptions. Each batch includes ~2% exact duplicates and ~3% invalid rows.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "sdp_demo")
dbutils.widgets.text("num_records", "10000")

MAX_RECORDS = 1_000_000

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
num_records = int(dbutils.widgets.get("num_records"))
assert 1 <= num_records <= MAX_RECORDS, f"num_records must be between 1 and {MAX_RECORDS:,}"
assert catalog, "catalog is required"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.raw")
path = f"/Volumes/{catalog}/{schema}/raw/subscriptions"

# COMMAND ----------

from pyspark.sql import functions as F

num_subscriptions = max(num_records // 4, 1)
num_customers = max(num_subscriptions // 2, 1)
num_duplicates = num_records // 50

events = (
    spark.range(num_records - num_duplicates)
    .withColumn("n", (F.rand() * num_subscriptions).cast("int") + 1)
    .withColumn("r", F.rand())
    .selectExpr(
        "id",
        "r",
        "format_string('SUB-%07d', n) AS subscription_id",
        f"format_string('CUST-%07d', pmod(hash(n, 'customer'), {num_customers}) + 1) AS customer_id",
        "element_at(array('free', 'starter', 'pro', 'enterprise'), cast(rand() * 4 AS INT) + 1) AS plan",
        "element_at(array('active', 'active', 'active', 'active', 'active', 'active', 'active', 'paused', 'cancelled', 'cancelled'), cast(rand() * 10 AS INT) + 1) AS status",
        "element_at(array('US', 'CA', 'GB', 'DE', 'ES', 'BR', 'IN'), pmod(hash(n, 'country'), 7) + 1) AS country",
        "date_sub(current_date(), pmod(hash(n, 'start'), 90)) AS start_date",
    )
    .selectExpr(
        "*",
        "CAST(CASE plan WHEN 'starter' THEN 29 WHEN 'pro' THEN 99 WHEN 'enterprise' THEN 499 ELSE 0 END AS DECIMAL(10,2)) AS monthly_price",
        "timestampadd(SECOND, CAST(rand() * (unix_timestamp() - unix_timestamp(start_date)) AS BIGINT), start_date) AS updated_at",
    )
    # Invalid rows for the silver expectations: missing id, unknown plan, negative price
    .selectExpr(
        "id",
        "CASE WHEN r < 0.01 THEN NULL ELSE subscription_id END AS subscription_id",
        "customer_id",
        "CASE WHEN r >= 0.01 AND r < 0.02 THEN 'trial' ELSE plan END AS plan",
        "status",
        "CASE WHEN r >= 0.02 AND r < 0.03 THEN -monthly_price - 1 ELSE monthly_price END AS monthly_price",
        "country",
        "start_date",
        "updated_at",
    )
    # Exact duplicates, as an at-least-once source would deliver them
    .withColumn("copy", F.explode(F.sequence(F.lit(1), F.when(F.col("id") < num_duplicates, 2).otherwise(1))))
    .drop("id", "copy")
)

events.coalesce(1).write.mode("append").json(path)
print(f"Wrote {num_records:,} events to {path}")
