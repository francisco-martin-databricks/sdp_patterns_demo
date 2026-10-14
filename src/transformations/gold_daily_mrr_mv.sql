-- Gold (materialized view): signups and current MRR by start date, plan and country; always exact
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_mrr_mv
COMMENT 'Signups and current MRR by start date, plan and country'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT
  start_date,
  plan,
  country,
  count(*) AS subscriptions,
  count_if(status = 'active') AS active_subscriptions,
  sum(CASE WHEN status = 'active' THEN monthly_price ELSE 0 END) AS mrr
FROM silver_subscriptions
GROUP BY start_date, plan, country;
