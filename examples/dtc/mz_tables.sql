-- This file contains the statements run against Materialize
-- at start up to power a real time feature store.

-- Create a new materialized source from the a postgres
-- replication bin log. This will continuously update
-- as the PostgreSQL tables are modified upstream.
CREATE MATERIALIZED SOURCE IF NOT EXISTS pg_source FROM POSTGRES
    CONNECTION 'host=postgres user=postgres dbname=default'
    PUBLICATION 'mz_source'
    WITH (timestamp_frequency_ms = 100);

-- From that source, create views for all tables being replicated.
-- This will include the leads and coupons tables.
CREATE VIEWS FROM SOURCE pg_source;

-- Create a new source to read conversion predictions
-- from the conversion_predictions topic on RedPanda.
CREATE SOURCE IF NOT EXISTS kafka_conversion_predictions
    FROM KAFKA BROKER 'redpanda:9092' TOPIC 'conversion_predictions'
    FORMAT BYTES;

-- Conversion predictions are encoded as JSON and consumed as raw bytes.
-- We can create a view to decode this into a well typed format, making
-- it easier to use.
CREATE VIEW IF NOT EXISTS conversion_predictions AS
  SELECT
    CAST(data->>'lead_id' AS BIGINT) AS lead_id
    , CAST(data->>'experiment_bucket' AS VARCHAR(255)) AS experiment_bucket
    , CAST(data->>'predicted_at' AS TIMESTAMP) AS predicted_at
    , CAST(data->>'score' AS FLOAT) AS score
    , CAST(data->>'label' AS INT) AS label
  FROM (SELECT CONVERT_FROM(data, 'utf8')::jsonb AS data FROM kafka_conversion_predictions);

-- At each second, calculate the dataset of conversion predictions and outcomes over 
-- the trailing 30 seconds.
CREATE VIEW IF NOT EXISTS conversion_prediction_dataset AS

WITH spine AS (
  SELECT
    leads.created_at AS timestamp 
    , leads.id AS lead_id 
  FROM leads 
  WHERE 
    -- The below conditions define "hopping windows" of period 2 seconds and window size 
    -- 30 seconds. Basically, every 2 seconds, we are looking at a trailing 30 second 
    -- window of data.
    -- See https://materialize.com/docs/sql/patterns/temporal-filters/#hopping-windows
    -- for more info
    mz_logical_timestamp() >= 2000 * (EXTRACT(EPOCH FROM leads.created_at)::bigint * 1000 / 2000)
    AND mz_logical_timestamp() < 30000 * (2000 + EXTRACT(EPOCH FROM leads.created_at)::bigint * 1000 / 2000) 
)

, predictions AS (
  SELECT
    spine.lead_id
    , conversion_predictions.experiment_bucket
    , conversion_predictions.predicted_at 
    , conversion_predictions.score 
    , conversion_predictions.label::BOOL
  FROM spine 
  LEFT JOIN conversion_predictions 
    ON conversion_predictions.lead_id = spine.lead_id 
)

, outcomes AS (
  SELECT 
    spine.lead_id 
    , CASE 
        WHEN
          leads.converted_at IS NULL THEN FALSE 
        WHEN
          leads.converted_at <= (leads.created_at + INTERVAL '30 seconds')
          THEN TRUE
        ELSE FALSE
      END AS value
    , CASE 
        WHEN
          leads.converted_at IS NULL THEN NULL 
        WHEN
          -- Make sure to only use conversion data that was known 
          -- _as of_ the lead created at second.
          leads.converted_at <= (leads.created_at + INTERVAL '30 seconds')
          THEN leads.converted_at 
        ELSE NULL 
      END AS lead_converted_at
    , CASE 
        WHEN 
          leads.converted_at IS NULL THEN NULL 
        WHEN
          leads.converted_at <= (leads.created_at + INTERVAL '30 seconds')
          THEN leads.conversion_amount 
        ELSE NULL 
      END AS conversion_amount
    , coupons.amount AS coupon_amount
  FROM spine 
  LEFT JOIN leads ON leads.id = spine.lead_id 
  LEFT JOIN coupons ON coupons.lead_id = spine.lead_id
)

SELECT 
  date_trunc('second', spine.timestamp) AS timestamp_second 
  , spine.lead_id 
  , predictions.experiment_bucket
  , predictions.score AS predicted_score
  , predictions.label AS predicted_value 
  , outcomes.value AS outcome_value
  , outcomes.conversion_amount 
  , outcomes.coupon_amount
FROM spine 
LEFT JOIN predictions ON predictions.lead_id = spine.lead_id 
LEFT JOIN outcomes ON outcomes.lead_id = spine.lead_id 
;

-- At each second, calculate the performance metrics of the
-- conversion prediction model over the trailing 30 seconds.
CREATE MATERIALIZED VIEW IF NOT EXISTS classifier_metrics AS

WITH aggregates AS (
  -- Calculate various performance metrics aggregations.
  SELECT
    timestamp_second
    , experiment_bucket
    , COUNT(DISTINCT lead_id) AS num_leads
    , SUM((predicted_value AND outcome_value)::INT) 
        AS true_positives
    , SUM((predicted_value AND not outcome_value)::INT) 
        AS false_positives
    , SUM((NOT predicted_value AND not outcome_value)::INT) 
        AS true_negatives
    , SUM((NOT predicted_value AND not outcome_value)::INT) 
        AS false_negatives
    , SUM(conversion_amount)::FLOAT / 100 AS conversion_revenue_dollars
    , (SUM(conversion_amount) - SUM(COALESCE(coupon_amount, 0)))::FLOAT / 100 
        AS net_conversion_revenue_dollars
  FROM conversion_prediction_dataset 
  GROUP BY 1, 2
)

-- Final metrics
SELECT
  timestamp_second 
  , experiment_bucket
  , num_leads 
  , true_positives 
  , false_positives 
  , true_negatives
  , false_negatives
  , conversion_revenue_dollars
  , net_conversion_revenue_dollars
  , true_positives::FLOAT 
    / NULLIF(true_positives + false_positives, 0) 
      AS precision 
  , true_positives::FLOAT 
    / NULLIF(true_positives + false_negatives, 0) 
      AS recall 
  , true_positives::FLOAT 
    / NULLIF(
        true_positives 
          + 1.0 / 2.0 * (false_positives + false_negatives)
        , 0
    ) 
      AS f1_score
FROM aggregates 
;
