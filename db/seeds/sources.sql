BEGIN;
INSERT INTO sources (name, url, type, effective_date) VALUES
('PSIX SOAP WSDL', 'https://cgmix.uscg.mil/xml/PSIXData.asmx?WSDL', 'api', '2015-01-01'),
('PSIX Vessel Search', 'https://cgmix.uscg.mil/psix/psixsearch.aspx', 'api', '2015-01-01'),
('COFR Active Vessel Status', 'https://www.uscg.mil/Mariners/National-Pollution-Funds-Center/COFRs/ECOFR-Active-Vessel-Status/', 'program', '2024-01-01'),
('CBP User Fee Table', 'https://www.cbp.gov/trade/basic-import-export/user-fee-table', 'law', '2025-04-21'),
('Federal Register FY26 User Fee Adjustments', 'https://www.federalregister.gov/documents/2025/07/23/2025-13869/customs-user-fees-to-be-adjusted-for-inflation-in-fiscal-year-2026-cbp-dec-25-10', 'law', '2025-07-23'),
('CA State Lands Commission – MISP', 'https://www.slc.ca.gov/misp/', 'program', '2025-01-01'),
('CDTFA – Marine Invasive Species Fee', 'https://cdtfa.ca.gov/taxes-and-fees/marine-invasive-species-fee/', 'program', '2025-06-05'),
('SF Bar Pilots – Operational Guidelines', 'https://sfbarpilots.com/new-operational/', 'pilotage', '2024-01-01'),
('Puget Sound Pilots – Guidelines', 'https://www.pspilots.org/dispatch-information/general-guidelines-for-vessels/', 'pilotage', '2024-01-01'),
('LA Pilot Service', 'https://www.portoflosangeles.org/business/pilot-service', 'pilotage', '2024-01-01'),
('Jacobsen/Long Beach Pilotage', 'https://www.jacobsenpilot.com/pilotage/', 'pilotage', '2024-01-01'),
('Marine Exchange SoCal', 'https://mxsocal.org/', 'exchange', '2024-01-01'),
('SF Marine Exchange / Harbor Safety', 'https://www.sfmx.org/bay-area-committees/hsc/', 'exchange', '2024-01-01'),
('Columbia River Pilots', 'https://colrip.com/', 'pilotage', '2024-01-01'),
('Port of Stockton Tariff', 'https://www.portofstockton.com/port-tariff/', 'tariff', '2024-01-01');
COMMIT;
