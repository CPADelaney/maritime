BEGIN;

-- === CBP User Fee — FY25
INSERT INTO fees (code, name, scope, unit, rate, cap_amount, cap_period, effective_start, effective_end, source_url, authority)
SELECT 'CBP_COMMERCIAL_VESSEL_ARRIVAL_FEE', 'CBP Commercial Vessel Arrival Fee', 'federal', 'per_call',
       571.81, 7792.05, 'calendar_year', DATE '2024-10-01', DATE '2025-09-30',
       'https://www.cbp.gov/trade/basic-import-export/user-fee-table', '19 CFR 24.22'
WHERE NOT EXISTS (
  SELECT 1 FROM fees
  WHERE code = 'CBP_COMMERCIAL_VESSEL_ARRIVAL_FEE'
    AND effective_start = DATE '2024-10-01'
);

-- === CBP User Fee — FY26 (effective 2025-10-01)
INSERT INTO fees (code, name, scope, unit, rate, cap_amount, cap_period, effective_start, effective_end, source_url, authority)
SELECT 'CBP_COMMERCIAL_VESSEL_ARRIVAL_FEE', 'CBP Commercial Vessel Arrival Fee', 'federal', 'per_call',
       587.03, 7999.40, 'calendar_year', DATE '2025-10-01', NULL,
       'https://www.federalregister.gov/documents/2025/07/23/2025-13869/customs-user-fees-to-be-adjusted-for-inflation-in-fiscal-year-2026-cbp-dec-25-10', '19 CFR 24.22'
WHERE NOT EXISTS (
  SELECT 1 FROM fees
  WHERE code = 'CBP_COMMERCIAL_VESSEL_ARRIVAL_FEE'
    AND effective_start = DATE '2025-10-01'
);

-- === APHIS AQI — standard
INSERT INTO fees (code, name, scope, unit, rate, effective_start, effective_end, source_url, authority)
SELECT 'APHIS_COMMERCIAL_VESSEL', 'APHIS AQI Commercial Vessel Fee', 'federal', 'per_call',
       2903.73, DATE '2025-04-21', NULL,
       'https://www.cbp.gov/trade/basic-import-export/user-fee-table', 'Food, Agriculture and Conservation Act; APHIS/CBP MOU'
WHERE NOT EXISTS (
  SELECT 1 FROM fees
  WHERE code = 'APHIS_COMMERCIAL_VESSEL'
    AND effective_start = DATE '2025-04-21'
    AND (applies_cascadia IS NULL OR applies_cascadia = FALSE)
);

-- === APHIS AQI — Cascadia/Great Lakes reduced
INSERT INTO fees (code, name, scope, unit, rate, applies_cascadia, effective_start, effective_end, source_url, authority)
SELECT 'APHIS_COMMERCIAL_VESSEL', 'APHIS AQI Commercial Vessel Fee (Cascadia/Great Lakes)', 'federal', 'per_call',
       837.51, TRUE, DATE '2025-04-21', NULL,
       'https://www.cbp.gov/trade/basic-import-export/user-fee-table', 'Food, Agriculture and Conservation Act; APHIS/CBP MOU'
WHERE NOT EXISTS (
  SELECT 1 FROM fees
  WHERE code = 'APHIS_COMMERCIAL_VESSEL'
    AND effective_start = DATE '2025-04-21'
    AND applies_cascadia IS TRUE
);

-- === CA MISP ballast program
INSERT INTO fees (code, name, scope, unit, rate, applies_state, effective_start, effective_end, source_url, authority)
SELECT 'CA_MISP_PER_VOYAGE', 'California Marine Invasive Species Program Fee', 'state', 'per_call',
       1000.00, 'CA', DATE '2020-01-01', NULL,
       'https://www.slc.ca.gov/misp/', 'PRC §71215; 2 CCR §2271; CDTFA'
WHERE NOT EXISTS (
  SELECT 1 FROM fees
  WHERE code = 'CA_MISP_PER_VOYAGE'
    AND effective_start = DATE '2020-01-01'
    AND applies_state = 'CA'
);

-- === Tonnage tax placeholder (per net ton)
INSERT INTO fees (code, name, scope, unit, rate, cap_period, effective_start, effective_end, source_url, authority)
SELECT 'TONNAGE_TAX_PER_TON', 'Tonnage Tax (per net ton, regime selectable)', 'federal', 'per_net_ton',
       0.06, 'tonnage_year', DATE '2018-01-01', NULL,
       'https://www.law.cornell.edu/cfr/text/19/4.20', '19 CFR 4.20'
WHERE NOT EXISTS (
  SELECT 1 FROM fees
  WHERE code = 'TONNAGE_TAX_PER_TON'
    AND effective_start = DATE '2018-01-01'
);

-- === Marine Exchange/VTS (SoCal example)
INSERT INTO fees (code, name, scope, unit, rate, applies_port_code, effective_start, effective_end, source_url, authority)
SELECT 'MX_VTS_PER_CALL', 'Marine Exchange/VTS Fee (example)', 'port', 'per_call',
       250.00, 'LALB', DATE '2020-01-01', NULL,
       'https://mxsocal.org/', 'Local tariff/arrangement'
WHERE NOT EXISTS (
  SELECT 1 FROM fees
  WHERE code = 'MX_VTS_PER_CALL'
    AND effective_start = DATE '2020-01-01'
    AND applies_port_code = 'LALB'
);

COMMIT;
