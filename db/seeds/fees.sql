BEGIN;
-- CBP Commercial Vessel Arrival User Fee — FY25 (through 2025‑09‑30)
INSERT INTO fees (code, name, scope, unit, rate, cap_amount, cap_period, effective_start, effective_end, source_url, authority)
VALUES ('CBP_COMMERCIAL_VESSEL_ARRIVAL_FEE', 'CBP Commercial Vessel Arrival Fee', 'federal', 'per_call', 571.81, 7792.05, 'calendar_year', '2024-10-01', '2025-09-30',
        'https://www.cbp.gov/trade/basic-import-export/user-fee-table', '19 CFR 24.22');

-- CBP Commercial Vessel Arrival User Fee — FY26 (effective 2025‑10‑01)
INSERT INTO fees (code, name, scope, unit, rate, cap_amount, cap_period, effective_start, effective_end, source_url, authority)
VALUES ('CBP_COMMERCIAL_VESSEL_ARRIVAL_FEE', 'CBP Commercial Vessel Arrival Fee', 'federal', 'per_call', 587.03, 7999.40, 'calendar_year', '2025-10-01', NULL,
        'https://www.federalregister.gov/documents/2025/07/23/2025-13869/customs-user-fees-to-be-adjusted-for-inflation-in-fiscal-year-2026-cbp-dec-25-10', '19 CFR 24.22');

-- APHIS AQI Commercial Vessel Fee — standard (non‑Cascadia/Great Lakes)
INSERT INTO fees (code, name, scope, unit, rate, effective_start, effective_end, source_url, authority)
VALUES ('APHIS_COMMERCIAL_VESSEL', 'APHIS AQI Commercial Vessel Fee', 'federal', 'per_call', 2903.73, '2025-04-21', NULL,
        'https://www.cbp.gov/trade/basic-import-export/user-fee-table', 'Food, Agriculture and Conservation Act; APHIS/CBP MOU');

-- APHIS AQI Commercial Vessel Fee — Cascadia/Great Lakes reduced rate (port‑scoped override)
INSERT INTO fees (code, name, scope, unit, rate, applies_cascadia, effective_start, effective_end, source_url, authority)
VALUES ('APHIS_COMMERCIAL_VESSEL', 'APHIS AQI Commercial Vessel Fee (Cascadia/Great Lakes)', 'federal', 'per_call', 837.51, TRUE, '2025-04-21', NULL,
        'https://www.cbp.gov/trade/basic-import-export/user-fee-table', 'Food, Agriculture and Conservation Act; APHIS/CBP MOU');

-- CA MISP ballast program fee (per qualifying voyage)
INSERT INTO fees (code, name, scope, unit, rate, applies_state, effective_start, effective_end, source_url, authority)
VALUES ('CA_MISP_PER_VOYAGE', 'California Marine Invasive Species Program Fee', 'state', 'per_call', 1000.00, 'CA', '2020-01-01', NULL,
        'https://www.slc.ca.gov/misp/', 'PRC §71215; 2 CCR §2271; CDTFA');

-- Placeholders for tonnage tax (you will refine per regime; engine multiplies by net tons)
INSERT INTO fees (code, name, scope, unit, rate, cap_period, effective_start, effective_end, source_url, authority)
VALUES ('TONNAGE_TAX_PER_TON', 'Tonnage Tax (per net ton, regime selectable)', 'federal', 'per_net_ton', 0.06, 'tonnage_year', '2018-01-01', NULL,
        'https://www.law.cornell.edu/cfr/text/19/4.20', '19 CFR 4.20');

-- Example Marine Exchange / VTS per‑call placeholder (SoCal)
INSERT INTO fees (code, name, scope, unit, rate, applies_port_code, effective_start, effective_end, source_url, authority)
VALUES ('MX_VTS_PER_CALL', 'Marine Exchange/VTS Fee (example)', 'port', 'per_call', 250.00, 'LALB', '2020-01-01', NULL,
        'https://mxsocal.org/', 'Local tariff/arrangement');
COMMIT;
