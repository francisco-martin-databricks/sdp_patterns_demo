-- Silver: current state per subscription (SCD Type 1), with quality rules on incoming changes
CREATE OR REFRESH STREAMING TABLE silver_subscriptions (
  CONSTRAINT valid_subscription_id EXPECT (subscription_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_plan EXPECT (plan IN ('free', 'starter', 'pro', 'enterprise')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_price EXPECT (monthly_price >= 0) ON VIOLATION DROP ROW
)
COMMENT 'Latest version of each subscription, deduplicated'
TBLPROPERTIES ('quality' = 'silver');

CREATE FLOW silver_subscriptions_cdc AS AUTO CDC INTO silver_subscriptions
FROM STREAM(bronze_subscriptions)
KEYS (subscription_id)
SEQUENCE BY updated_at
COLUMNS * EXCEPT (_rescued_data, _source_file, _ingested_at)
STORED AS SCD TYPE 1;
