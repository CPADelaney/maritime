BEGIN;
CREATE TABLE IF NOT EXISTS ports (
  id SERIAL PRIMARY KEY,
  code VARCHAR(12) UNIQUE NOT NULL,
  name VARCHAR(120) NOT NULL,
  state CHAR(2),
  country CHAR(2) DEFAULT 'US',
  region VARCHAR(24),
  is_california BOOLEAN DEFAULT FALSE,
  is_cascadia BOOLEAN DEFAULT FALSE,
  pilotage_url VARCHAR(512),
  mx_url VARCHAR(512),
  tariff_url VARCHAR(512)
);

CREATE TABLE IF NOT EXISTS port_documents (
  id SERIAL PRIMARY KEY,
  port_code VARCHAR(12) NOT NULL,
  document_name VARCHAR(200) NOT NULL,
  document_code VARCHAR(64),
  is_mandatory BOOLEAN DEFAULT TRUE NOT NULL,
  lead_time_hours INTEGER DEFAULT 0,
  authority VARCHAR(200),
  description TEXT,
  applies_to_vessel_types TEXT[],
  applies_if_foreign BOOLEAN DEFAULT FALSE
);

CREATE UNIQUE INDEX IF NOT EXISTS port_documents_unique_doc
  ON port_documents (port_code, document_name, COALESCE(document_code, ''));

CREATE INDEX IF NOT EXISTS port_documents_port_code_idx
  ON port_documents (port_code);

CREATE TABLE IF NOT EXISTS fees (
  id SERIAL PRIMARY KEY,
  code VARCHAR(64) UNIQUE NOT NULL,
  name VARCHAR(200) NOT NULL,
  scope VARCHAR(24) NOT NULL,
  unit VARCHAR(24) NOT NULL,
  rate NUMERIC(12,4) NOT NULL,
  currency CHAR(3) DEFAULT 'USD' NOT NULL,
  cap_amount NUMERIC(12,4),
  cap_period VARCHAR(24),
  applies_state CHAR(2),
  applies_port_code VARCHAR(12),
  applies_cascadia BOOLEAN,
  effective_start DATE NOT NULL,
  effective_end DATE,
  source_url VARCHAR(512),
  authority VARCHAR(512)
);

CREATE TABLE IF NOT EXISTS sources (
  id SERIAL PRIMARY KEY,
  name VARCHAR(200) NOT NULL,
  url VARCHAR(512) NOT NULL,
  type VARCHAR(24) NOT NULL,
  effective_date DATE
);
COMMIT;
