-- Part A: 
--
-- I used Claude to help me covert the T-SQL syntax to BigQuery SQL


-- 1. Current version of each booking

CREATE OR REPLACE VIEW flux.v_bookings_current AS
SELECT *
FROM flux.bookings_clean
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY booking_id
  ORDER BY updated_at DESC, batch_date DESC, source_row DESC
) = 1;


-- 2. Point-in-time revenue in ZAR

CREATE OR REPLACE VIEW flux.v_bookings_zar AS
SELECT
  b.*,
  fx.rate_to_zar AS fx_rate_to_zar,
  ROUND(b.revenue * fx.rate_to_zar, 2) AS revenue_zar
FROM flux.v_bookings_current AS b
LEFT JOIN flux.fx_rates AS fx
  ON fx.currency = b.currency
 AND fx.effective_from <= DATE(b.created_at)
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY b.booking_id ORDER BY fx.effective_from DESC) = 1;


-- 3. Reporting model -----------------------------------------------------------
-- PARTITION BY month of check_in: monthly trend queries filter on the stay
--   month, so BigQuery only reads the months asked for. Monthly rather than
--   daily keeps partitions a sensible size.
-- CLUSTER BY property_id, booking_channel, booking_status: the next most common
--   filters (per-property, per-channel, confirmed only), so those queries read
--   fewer blocks inside each partition.
-- Only valid bookings are loaded (property exists, FX rate found, revenue > 0,
-- real date range, sensible guest count)
CREATE OR REPLACE TABLE flux.fct_bookings
PARTITION BY DATE_TRUNC(check_in, MONTH)
CLUSTER BY property_id, booking_channel, booking_status
AS
SELECT
  b.booking_id,
  b.property_id,
  b.booking_channel,
  b.booking_status,
  DATE(b.check_in) AS check_in,
  DATE(b.check_out) AS check_out,
  DATE_DIFF(DATE(b.check_out), DATE(b.check_in), DAY) AS nights,
  b.num_guests,
  b.currency,
  b.revenue,
  b.fx_rate_to_zar,
  b.revenue_zar,
  IF(b.booking_status = 'confirmed', b.revenue_zar, 0) AS recognised_revenue_zar,
  b.created_at,
  b.updated_at
FROM flux.v_bookings_zar AS b
JOIN flux.properties AS p USING (property_id)
WHERE b.revenue_zar > 0
  AND b.check_out > b.check_in
  AND b.num_guests BETWEEN 1 AND 12;

-- Reusable view for analysts: fact + property details + month.
CREATE OR REPLACE VIEW flux.v_bookings_reporting AS
SELECT
  f.*,
  DATE_TRUNC(f.check_in, MONTH) AS stay_month,
  p.property_name,
  p.country,
  p.region
FROM flux.fct_bookings AS f
JOIN flux.properties AS p USING (property_id);


-- 4. Month-on-month revenue growth, confirmed bookings, 2025 
WITH months AS (
  SELECT month
  FROM UNNEST(GENERATE_DATE_ARRAY('2025-01-01', '2025-12-01', INTERVAL 1 MONTH)) AS month
),
revenue AS (
  SELECT stay_month AS month, SUM(recognised_revenue_zar) AS revenue_zar
  FROM flux.v_bookings_reporting
  WHERE booking_status = 'confirmed'
    AND stay_month BETWEEN '2025-01-01' AND '2025-12-01'
  GROUP BY month
)
SELECT
  m.month,
  COALESCE(r.revenue_zar, 0) AS revenue_zar,
  ROUND(100 * SAFE_DIVIDE(
    COALESCE(r.revenue_zar, 0) - LAG(COALESCE(r.revenue_zar, 0)) OVER (ORDER BY m.month),
    LAG(COALESCE(r.revenue_zar, 0)) OVER (ORDER BY m.month)), 1) AS mom_growth_pct
FROM months AS m
LEFT JOIN revenue AS r USING (month)
ORDER BY m.month;


-- 5. Top 2 properties by revenue in each country
SELECT
  country,
  property_name,
  SUM(recognised_revenue_zar) AS revenue_zar,
  RANK() OVER (PARTITION BY country ORDER BY SUM(recognised_revenue_zar) DESC) AS revenue_rank
FROM flux.v_bookings_reporting
WHERE booking_status = 'confirmed'
GROUP BY country, property_name
QUALIFY revenue_rank <= 2
ORDER BY country, revenue_rank;


-- 6. Data-quality checks on the RAW data (one row per check) 
WITH raw AS (
  SELECT
    booking_id,
    property_id,
    COALESCE(SAFE.PARSE_DATE('%Y-%m-%d', TRIM(check_in)),
             SAFE.PARSE_DATE('%d/%m/%Y', TRIM(check_in)),
             SAFE.PARSE_DATE('%d %b %Y', TRIM(check_in))) AS check_in,
    COALESCE(SAFE.PARSE_DATE('%Y-%m-%d', TRIM(check_out)),
             SAFE.PARSE_DATE('%d/%m/%Y', TRIM(check_out)),
             SAFE.PARSE_DATE('%d %b %Y', TRIM(check_out))) AS check_out,
    SAFE_CAST(REPLACE(revenue, ',', '') AS NUMERIC) AS revenue,
    CASE LOWER(TRIM(currency))
      WHEN 'zar' THEN 'ZAR' WHEN 'r' THEN 'ZAR'
      WHEN 'usd' THEN 'USD' WHEN 'us$' THEN 'USD' WHEN '$' THEN 'USD'
      WHEN 'eur' THEN 'EUR' WHEN '€' THEN 'EUR'
      WHEN 'gbp' THEN 'GBP' WHEN '£' THEN 'GBP'
    END AS currency,
    SAFE_CAST(created_at AS DATETIME) AS created_at,
    SAFE_CAST(updated_at AS DATETIME) AS updated_at
  FROM flux.bookings_raw
),
current_version AS (  -- rows sharing the newest updated_at for their booking
  SELECT booking_id
  FROM raw
  WHERE TRUE
  QUALIFY updated_at = MAX(updated_at) OVER (PARTITION BY booking_id)
)
SELECT 'duplicate_current_version_ids' AS check_name,
       (SELECT COUNT(*) FROM (SELECT booking_id FROM current_version
                              GROUP BY booking_id HAVING COUNT(*) > 1)) AS failing_rows
UNION ALL
SELECT 'orphan_properties',
       COUNTIF(property_id NOT IN (SELECT property_id FROM flux.properties)) FROM raw
UNION ALL
SELECT 'missing_or_non_positive_revenue', COUNTIF(revenue IS NULL OR revenue <= 0) FROM raw
UNION ALL
SELECT 'invalid_date_range', COUNTIF(check_in IS NULL OR check_out IS NULL OR check_out <= check_in) FROM raw
UNION ALL
SELECT 'updated_before_created', COUNTIF(updated_at < created_at) FROM raw
UNION ALL
SELECT 'no_applicable_fx_rate',
       COUNTIF(NOT EXISTS (SELECT 1 FROM flux.fx_rates AS fx
                           WHERE fx.currency = raw.currency
                             AND fx.effective_from <= DATE(raw.created_at)))
FROM raw;
