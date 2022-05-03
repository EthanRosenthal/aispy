-- The table containing information about leads signing up for our SAAS product.
CREATE TABLE leads (
  id                SERIAL PRIMARY KEY,
  email             TEXT NOT NULL,
  created_at        TIMESTAMP NOT NULL DEFAULT NOW(),
  converted_at      TIMESTAMP,
  conversion_amount INT,
  utm_medium        TEXT,
  utm_source        TEXT
);

-- For Materielize to continously watch the changes
-- to the leads table, it must have
-- full replication
ALTER TABLE leads REPLICA IDENTITY FULL;

-- Create user and role to be used by Materialize
CREATE ROLE loadgen REPLICATION LOGIN PASSWORD 'materialize';
GRANT SELECT, INSERT, UPDATE ON leads TO loadgen;

-- The table containing information about coupons offered to leads.
CREATE TABLE coupons (
  id                SERIAL PRIMARY KEY,
  created_at        TIMESTAMP NOT NULL DEFAULT NOW(),
  lead_id           INT NOT NULL,
  -- amount is in cents.
  amount            INT NOT NULL
);

-- For Materielize to continously watch the changes
-- to the coupons table, it must have
-- full replication
ALTER TABLE coupons REPLICA IDENTITY FULL;

GRANT SELECT, INSERT, UPDATE ON coupons TO loadgen;

-- I think this is required for indexes.
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO loadgen;

-- The table will additionally be part of a publication
-- that Materialize will watch.
CREATE PUBLICATION mz_source FOR ALL TABLES;
