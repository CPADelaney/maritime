import os
import httpx
from zeep import Client, Settings
from zeep.transports import Transport

PSIX_WSDL = os.getenv("PSIX_WSDL", "https://cgmix.uscg.mil/xml/PSIXData.asmx?WSDL")
VERIFY_SSL = os.getenv("PSIX_VERIFY_SSL", "false").lower() in ("1","true","yes","y")
TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

class PsixClient:
    """
    Minimal PSIX SOAP client.

    Methods mirrored from CGMIX PSIXData.asmx:
      - getVesselSummary(VesselID, VesselName, CallSign, VIN, HIN, Flag, Service, BuildYear)
      - getVesselCases(VesselID)
      - getVesselDeficiencies(VesselID, ActivityNumber)
    """
    def __init__(self, wsdl: str | None = None, verify_ssl: bool | None = None, timeout: int | None = None):
        wsdl = wsdl or PSIX_WSDL
        verify_ssl = VERIFY_SSL if verify_ssl is None else verify_ssl
        timeout = TIMEOUT if timeout is None else timeout
        transport = Transport(client=httpx.Client(verify=verify_ssl, timeout=timeout))
        settings = Settings(strict=False, xml_huge_tree=True)
        self.client = Client(wsdl=wsdl, transport=transport, settings=settings)

    def get_vessel_summary(self, *, vessel_id: int | None = None, vessel_name: str = "", call_sign: str = "", vin: str = "", hin: str = "", flag: str = "", service: str = "", build_year: str = ""):
        vid = vessel_id if vessel_id is not None else ""
        return self.client.service.getVesselSummary(
            VesselID=vid,
            VesselName=vessel_name,
            CallSign=call_sign,
            VIN=vin,
            HIN=hin,
            Flag=flag,
            Service=service,
            BuildYear=build_year,
        )

    def search_by_name(self, name: str):
        return self.get_vessel_summary(vessel_name=name)

    def list_cases(self, vessel_id: int):
        return self.client.service.getVesselCases(VesselID=vessel_id)

    def list_deficiencies(self, vessel_id: int, activity_number: str):
        return self.client.service.getVesselDeficiencies(VesselID=vessel_id, ActivityNumber=activity_number)
