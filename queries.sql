-- Example DuckDB queries for the data warehouse
-- Replace YOUR_BUCKET with your actual S3 bucket name
--
-- First, configure S3 credentials in DuckDB:
--   INSTALL httpfs; LOAD httpfs;
--   SET s3_region='us-east-1';
--   SET s3_access_key_id='...';
--   SET s3_secret_access_key='...';

-- Record counts per table
SELECT 'customers' AS table_name, count(*) AS records FROM read_parquet('s3://YOUR_BUCKET/customers/**/*.parquet')
UNION ALL
SELECT 'orders', count(*) FROM read_parquet('s3://YOUR_BUCKET/orders/**/*.parquet')
UNION ALL
SELECT 'events', count(*) FROM read_parquet('s3://YOUR_BUCKET/events/**/*.parquet');

-- Revenue by month
SELECT
  date_trunc('month', created_at::TIMESTAMP) AS month,
  count(*) AS order_count,
  sum(amount) / 100.0 AS revenue
FROM read_parquet('s3://YOUR_BUCKET/orders/**/*.parquet')
WHERE status = 'completed'
GROUP BY month
ORDER BY month;

-- Revenue by customer plan
SELECT
  c.metadata->>'plan' AS plan,
  count(DISTINCT c.id) AS customers,
  count(o.id) AS orders,
  sum(o.amount) / 100.0 AS total_revenue
FROM read_parquet('s3://YOUR_BUCKET/customers/**/*.parquet') c
JOIN read_parquet('s3://YOUR_BUCKET/orders/**/*.parquet') o ON c.id = o.customer_id
WHERE o.status = 'completed'
GROUP BY plan
ORDER BY total_revenue DESC;

-- Top 10 customers by spend
SELECT
  c.name,
  c.email,
  c.metadata->>'plan' AS plan,
  count(o.id) AS order_count,
  sum(o.amount) / 100.0 AS total_spent
FROM read_parquet('s3://YOUR_BUCKET/customers/**/*.parquet') c
JOIN read_parquet('s3://YOUR_BUCKET/orders/**/*.parquet') o ON c.id = o.customer_id
WHERE o.status = 'completed'
GROUP BY c.name, c.email, plan
ORDER BY total_spent DESC
LIMIT 10;

-- Most used features
SELECT
  properties->>'feature' AS feature,
  count(*) AS usage_count,
  count(DISTINCT customer_id) AS unique_users
FROM read_parquet('s3://YOUR_BUCKET/events/**/*.parquet')
WHERE event_name = 'feature_used'
GROUP BY feature
ORDER BY usage_count DESC;

-- Event breakdown by type
SELECT
  event_name,
  count(*) AS total,
  count(DISTINCT customer_id) AS unique_users
FROM read_parquet('s3://YOUR_BUCKET/events/**/*.parquet')
GROUP BY event_name
ORDER BY total DESC;

-- Monthly active users (by login events)
SELECT
  date_trunc('month', created_at::TIMESTAMP) AS month,
  count(DISTINCT customer_id) AS active_users
FROM read_parquet('s3://YOUR_BUCKET/events/**/*.parquet')
WHERE event_name = 'login'
GROUP BY month
ORDER BY month;

-- Customers who export the most data
SELECT
  c.name,
  c.email,
  count(*) AS export_count,
  sum((e.properties->>'rows')::INT) AS total_rows_exported
FROM read_parquet('s3://YOUR_BUCKET/events/**/*.parquet') e
JOIN read_parquet('s3://YOUR_BUCKET/customers/**/*.parquet') c ON e.customer_id = c.id
WHERE e.event_name = 'export_data'
GROUP BY c.name, c.email
ORDER BY total_rows_exported DESC;

-- Customers with orders but no recent activity (last 90 days)
SELECT
  c.name,
  c.email,
  c.metadata->>'plan' AS plan,
  max(e.created_at) AS last_activity,
  sum(o.amount) / 100.0 AS total_spent
FROM read_parquet('s3://YOUR_BUCKET/customers/**/*.parquet') c
JOIN read_parquet('s3://YOUR_BUCKET/orders/**/*.parquet') o ON c.id = o.customer_id
LEFT JOIN read_parquet('s3://YOUR_BUCKET/events/**/*.parquet') e ON c.id = e.customer_id
GROUP BY c.name, c.email, plan
HAVING max(e.created_at)::TIMESTAMP < current_timestamp - INTERVAL '90 days'
ORDER BY total_spent DESC;
