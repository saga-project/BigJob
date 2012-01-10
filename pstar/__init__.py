application_id = "bigjob"


from pstar.impl.pilotjob_manager import PilotJobService as MyPilotJobService
from pstar.impl.pstar_manager import WorkDataService as MyWorkDataService
from pstar.api.compute.api import State as MyState

# define external-facing API

class PilotJobService(MyPilotJobService):
    pass


class WorkDataService(MyWorkDataService):
    pass


class State(MyState):
    pass


