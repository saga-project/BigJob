from pilot.api.compute.api import PilotCompute as MyPilotCompute
from pilot.api.compute.api import PilotComputeService as MyPilotComputeService
from pilot.api.compute.api import PilotComputeDescription as MyPilotComputeDescription
from pilot.api.compute.api import ComputeUnit as MyComputeUnit
from pilot.api.compute.api import ComputeUnitService as MyComputeUnitService
from pilot.api.compute.api import ComputeUnitDescription as MyComputeUnitDescription

from pilot.api.compute.api import State as MyState

from pilot.api.data.api import PilotDataDescription as MyPilotDataDescription
from pilot.api.data.api import PilotData as MyPilotData
from pilot.api.data.api import PilotDataService as MyPilotDataService
from pilot.api.data.api import DataUnitService as MyDataUnitService
from pilot.api.data.api import DataUnit as MyDataUnit
from pilot.api.data.api import DataUnitDescription as MyDataUnitDescription

from pilot.api.api import ComputeDataDescription as MyComputeDataDescription
from pilot.api.api import ComputeDataService as MyComputeDataService

###############################################################################
# Common state model

class State(MyState):
    pass


###############################################################################
# Compute only

class PilotCompute(MyPilotCompute):
    pass


class PilotComputeService(MyPilotComputeService):
    pass


class PilotComputeDescription(MyPilotComputeDescription):
    pass


class ComputeUnit(MyComputeUnit):
    pass


class ComputeUnitService(MyComputeUnit):
    pass


class ComputeUnitDescription(MyComputeUnitDescription):
    pass


###############################################################################
# Data only

class PilotData(MyPilotData):
    pass


class PilotDataService(MyPilotDataService):
    pass


class PilotDataDescription(MyPilotDataDescription):
    pass


class DataUnit(MyDataUnit):
    pass


class DataUnitService(MyDataUnitService):
    pass


class DataUnitDescription(MyDataUnitDescription):
    pass



###############################################################################
# Compute and Data

class ComputeDataDescription(MyComputeDataDescription):
    pass


class ComputeDataService(MyComputeDataService):
    pass
