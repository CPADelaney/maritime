BEGIN;
-- Core USWC focus ports (codes are internal labels for now)
INSERT INTO ports (code, name, state, region, is_california, is_cascadia, pilotage_url, mx_url, tariff_url) VALUES
('LALB', 'Los Angeles / Long Beach', 'CA', 'SoCal', TRUE, FALSE,
 'https://www.portoflosangeles.org/business/pilot-service',
 'https://mxsocal.org/',
 'https://www.polb.com/port-info/tariff/'),
('SFBAY', 'San Francisco Bay', 'CA', 'NorCal', TRUE, FALSE,
 'https://sfbarpilots.com/new-operational/',
 'https://www.sfmx.org/bay-area-committees/hsc/',
 NULL),
('PUGET', 'Puget Sound (Seattle/Tacoma)', 'WA', 'PNW', FALSE, TRUE,
 'https://www.pspilots.org/dispatch-information/general-guidelines-for-vessels/',
 'https://marexps.com/',
 NULL),
('COLRIV', 'Columbia River (Astoria/Portland)', 'OR', 'PNW', FALSE, TRUE,
 'https://colrip.com/',
 'https://www.pdxmex.com/resources/',
 NULL),
('STKN', 'Port of Stockton', 'CA', 'Inland-Delta', TRUE, FALSE,
 NULL,
 NULL,
 'https://www.portofstockton.com/port-tariff/');
COMMIT;
