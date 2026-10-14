-- Bronze: append every new file that lands in the volume; protected from full refresh
CREATE OR REFRESH STREAMING TABLE bronze_subscriptions
COMMENT 'Raw subscription change events, appended as files land in the volume'
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.reset.allowed' = 'false'
)
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${source_path}',
  format => 'json',
  schemaHints => 'monthly_price DECIMAL(10,2), start_date DATE, updated_at TIMESTAMP'
);
