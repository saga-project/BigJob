from pstar.api.compute.api import PilotCompute as MyPilotCompute
from pstar.api.compute.api import PilotComputeService as MyPilotComputeService
from pstar.api.api import ComputeDataDescription as MyComputeDataDescription
from pstar.api.api import ComputeDataService as MyComputeDataService


class PilotCompute(MyPilotCompute):
    pass


class PilotComputeService(MyPilotComputeService):
    pass


class ComputeDataDescription(MyComputeDataDescription):
    pass


class ComputeDataService(MyComputeDataService):
    pass
