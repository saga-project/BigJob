application_id = "bigjob"


from pilot.impl.pilotcompute_manager import PilotComputeService as MyPilotComputeService
from pilot.impl.pilotdata_manager import PilotDataService as MyPilotDataService
from pilot.impl.pilotdata_manager import DataUnit as MyDataUnit
from pilot.impl.pilot_manager import ComputeDataService as MyComputeDataService
from pilot.impl.pilot_manager import ComputeUnit as MyComputeUnit
from pilot.api import State as MyState

# define external-facing API

class PilotComputeService(MyPilotComputeService):
    pass


class PilotDataService(MyPilotDataService):
    pass


class ComputeUnit(MyComputeUnit):
    pass

class DataUnit(MyDataUnit):
    pass


class ComputeDataService(MyComputeDataService):
    pass


class State(MyState):
    pass


