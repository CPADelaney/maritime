BEGIN;

INSERT INTO port_zones (code, name, region, primary_state, country, description) VALUES
  ('SOCAL', 'Southern California', 'SoCal', 'CA', 'US', 'Ports and harbors along the greater Los Angeles and Long Beach complex.'),
  ('NORCAL', 'San Francisco Bay Area', 'NorCal', 'CA', 'US', 'Bay Area port authorities serving the San Francisco and Oakland waterfronts.'),
  ('PUGET', 'Puget Sound', 'PNW', 'WA', 'US', 'Salish Sea ports from Seattle north to Bellingham, Washington.'),
  ('COLUMBIA', 'Columbia River', 'PNW', 'OR', 'US', 'Lower Columbia River and Willamette River deep-draft ports.'),
  ('INLAND', 'California Delta & Inland Waterways', 'Inland-Delta', 'CA', 'US', 'Deep-draft inland river ports connected to the San Joaquin River.')
ON CONFLICT (code) DO UPDATE
SET name = EXCLUDED.name,
    region = EXCLUDED.region,
    primary_state = EXCLUDED.primary_state,
    country = EXCLUDED.country,
    description = EXCLUDED.description;

COMMIT;
