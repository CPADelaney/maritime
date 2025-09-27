BEGIN;

-- Core USWC focus ports linked to geographic zones
INSERT INTO ports (code, name, state, country, region, is_california, is_cascadia, pilotage_url, mx_url, tariff_url, zone_id) VALUES
('LALB', 'Los Angeles / Long Beach', 'CA', 'US', 'SoCal', TRUE, FALSE,
 'https://www.portoflosangeles.org/business/pilot-service',
 'https://mxsocal.org/',
 'https://www.polb.com/port-info/tariff/',
 (SELECT id FROM port_zones WHERE code = 'SOCAL')),
('SFBAY', 'San Francisco Bay', 'CA', 'US', 'NorCal', TRUE, FALSE,
 'https://sfbarpilots.com/new-operational/',
 'https://www.sfmx.org/bay-area-committees/hsc/',
 NULL,
 (SELECT id FROM port_zones WHERE code = 'NORCAL')),
('PUGET', 'Puget Sound (Seattle/Tacoma)', 'WA', 'US', 'PNW', FALSE, TRUE,
 'https://www.pspilots.org/dispatch-information/general-guidelines-for-vessels/',
 'https://marexps.com/',
 NULL,
 (SELECT id FROM port_zones WHERE code = 'PUGET')),
('BELLINGHAM', 'Port of Bellingham', 'WA', 'US', 'PNW', FALSE, TRUE,
 'https://www.pspilots.org/dispatch-information/general-guidelines-for-vessels/',
 NULL,
 'https://www.portofbellingham.com/DocumentCenter/View/5930/Tariff-No-1',
 (SELECT id FROM port_zones WHERE code = 'PUGET')),
('COLRIV', 'Columbia River (Astoria/Portland)', 'OR', 'US', 'PNW', FALSE, TRUE,
 'https://colrip.com/',
 'https://www.pdxmex.com/resources/',
 NULL,
 (SELECT id FROM port_zones WHERE code = 'COLUMBIA')),
('STKN', 'Port of Stockton', 'CA', 'US', 'Inland-Delta', TRUE, FALSE,
 NULL,
 NULL,
 'https://www.portofstockton.com/port-tariff/',
 (SELECT id FROM port_zones WHERE code = 'INLAND'))
ON CONFLICT (code) DO UPDATE SET
  name = EXCLUDED.name,
  state = EXCLUDED.state,
  country = EXCLUDED.country,
  region = EXCLUDED.region,
  is_california = EXCLUDED.is_california,
  is_cascadia = EXCLUDED.is_cascadia,
  pilotage_url = EXCLUDED.pilotage_url,
  mx_url = EXCLUDED.mx_url,
  tariff_url = EXCLUDED.tariff_url,
  zone_id = EXCLUDED.zone_id;

COMMIT;
