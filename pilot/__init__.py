application_id = "bigjob"


from pilot.impl.pilotcompute_manager import PilotComputeService as MyPilotComputeService
from pilot.impl.pilot_manager import ComputeDataService as MyComputeDataService
from pilot.api import State as MyState

# define external-facing API

class PilotComputeService(MyPilotComputeService):
    pass


class ComputeDataService(MyComputeDataService):
    pass


class State(MyState):
    pass


