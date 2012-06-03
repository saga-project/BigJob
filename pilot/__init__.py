application_id = "bigjob"

from pilot.impl.pilotcompute_manager import PilotComputeService
from pilot.impl.pilotcompute_manager import PilotCompute
from pilot.impl.pilotdata_manager import PilotDataService
from pilot.impl.pilotdata_manager import PilotData
from pilot.impl.pilotdata_manager import DataUnit
from pilot.impl.pilot_manager import ComputeDataService 
from pilot.impl.pilot_manager import ComputeUnit 
from pilot.api.api import ComputeDataDescription
from pilot.api import State 


class ComputeUnitService():
    
    def __init__(self):
        raise NotImplementedError("Please use ComputeDataService.")


class DataUnitService():
    
    def __init__(self):
        raise NotImplementedError("Please use ComputeDataService.")

