BEGIN;

CREATE TABLE IF NOT EXISTS port_zones (
  id SERIAL PRIMARY KEY,
  code VARCHAR(12) UNIQUE NOT NULL,
  name VARCHAR(120) NOT NULL,
  region VARCHAR(48),
  primary_state CHAR(2),
  country CHAR(2) DEFAULT 'US',
  description TEXT
);

CREATE TABLE IF NOT EXISTS ports (
  id SERIAL PRIMARY KEY,
  zone_id INTEGER REFERENCES port_zones (id) ON UPDATE CASCADE ON DELETE SET NULL,
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

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'ports' AND column_name = 'zone_id'
  ) THEN
    ALTER TABLE ports ADD COLUMN zone_id INTEGER;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'ports_zone_id_fkey'
  ) THEN
    ALTER TABLE ports
      ADD CONSTRAINT ports_zone_id_fkey FOREIGN KEY (zone_id)
      REFERENCES port_zones (id)
      ON UPDATE CASCADE ON DELETE SET NULL;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS ports_zone_id_idx
  ON ports (zone_id);

CREATE TABLE IF NOT EXISTS terminals (
  id SERIAL PRIMARY KEY,
  port_id INTEGER NOT NULL REFERENCES ports (id) ON DELETE CASCADE,
  code VARCHAR(24) UNIQUE NOT NULL,
  name VARCHAR(200) NOT NULL,
  operator_name VARCHAR(200),
  is_public BOOLEAN DEFAULT FALSE,
  notes TEXT
);

CREATE INDEX IF NOT EXISTS terminals_port_id_idx
  ON terminals (port_id);

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
