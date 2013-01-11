import compute.api
import data.api

class PilotError(Exception):
    def __init__(self, value):
        self.value = value
    
    def __str__(self):
        return repr(self.value)


class ComputeDataService(compute.api.ComputeUnitService, data.api.PilotDataService):
    """ B{ComputeDataService (CDS).}
    
        The ComputeDataService is the application's interface to submit 
        ComputeUnits and PilotData/DataUnit to the Pilot-Manager 
        in the P* Model.        
    """
   

    def __init__(self, wds_id=None):
        """ Create a Compute Data Service object.

            Keyword arguments:
            wds_id -- Reconnect to an existing WDS (optional).
        """
        raise  NotImplementedError("Abstract super class, please use ComputeDataService implementation class in pilot namespace")
   

