BEGIN;

INSERT INTO terminals (code, port_id, name, operator_name, is_public, notes) VALUES
  ('LALB-PIER400', (SELECT id FROM ports WHERE code = 'LALB'), 'Pier 400', 'APM Terminals', FALSE, 'Privately operated marine terminal within the Los Angeles / Long Beach complex.'),
  ('LALB-PIERJ', (SELECT id FROM ports WHERE code = 'LALB'), 'Pier J', 'Port of Long Beach', TRUE, 'Municipally owned berth managed by the Port of Long Beach.'),
  ('BLI-CRUISE', (SELECT id FROM ports WHERE code = 'BELLINGHAM'), 'Bellingham Cruise Terminal', 'Port of Bellingham', TRUE, 'Publicly owned gateway for Alaska Marine Highway and seasonal cruise traffic.')
ON CONFLICT (code) DO UPDATE SET
  port_id = EXCLUDED.port_id,
  name = EXCLUDED.name,
  operator_name = EXCLUDED.operator_name,
  is_public = EXCLUDED.is_public,
  notes = EXCLUDED.notes;

COMMIT;
