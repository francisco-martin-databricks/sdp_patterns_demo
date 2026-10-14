-- Gold (Replace Where): same aggregate, but each run recomputes only the last 7 days of start dates
CREATE OR REFRESH STREAMING TABLE gold_daily_mrr_rw
COMMENT 'Signups and MRR by start date, plan and country; last 7 days recomputed per run'
TBLPROPERTIES ('quality' = 'gold')
FLOW REPLACE WHERE start_date >= date_add(current_date(), -7) BY NAME
SELECT
  start_date,
  plan,
  country,
  count(*) AS subscriptions,
  count_if(status = 'active') AS active_subscriptions,
  sum(CASE WHEN status = 'active' THEN monthly_price ELSE 0 END) AS mrr
FROM silver_subscriptions
GROUP BY start_date, plan, country;
