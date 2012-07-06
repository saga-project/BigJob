''' B{BigJob Documentation}

This is the BigJob implementation of the Pilot API. Pilot-Jobs (also referred to as
Pilot Compute) decouple system level and application level job management.  

The main concepts and classes exposed by the Compute part of the API are:

    - L{PilotCompute} (PC): 
          a pilot job, which can execute some compute workload (L{ComputeUnit}).

    - L{PilotComputeDescription} (PCD): 
          description for specifying the requirements of a \L{PilotCompute}.  

    - L{PilotComputeService} (PCS): 
          a factory for creating \L{PilotCompute}s.  


The application workload is represented by so called  L{ComputeUnit}s and  L{DataUnit}s: 
   
    - L{ComputeUnit} (CU): 
          a work item executed on a L{PilotCompute}.

    
    - L{DataUnit} (DU): 
          a data item managed by a L{PilotData}


The Data side of the Pilot API looks symmetric to the compute side.  The exposed
classes are:

    - L{PilotData} (PD): 
          a pilot that manages some data workload (L{DataUnit})

    - L{PilotDataService} (PDS): 
          a factory (service) which can create L{PilotData}s according to some
          specification

The L{ComputeDataService} represents the central entry point for the application
workload:

    - L{ComputeDataService} (CDS)
          a service which can map CUs and DUs to a set of Pilot Compute and Pilot Data.

The L{ComputeDataService} (CDS) takes care of the placement of Compute and Data Units. 
The set of L{PilotCompute}s and L{PilotData} available to the CDS can be changed during 
the application's runtime.  The CDS different data-compute affinity and will handle
compute/data co-locationing for the requested data-compute workload.

B{Example}::

    from pilot import PilotComputeService, ComputeDataService, State
    
    pilot_compute_service = PilotComputeService()

    # create pilot job service and initiate a pilot job
    pilot_compute_description = {
                             "service_url": 'fork://localhost',
                             "number_of_processes": 1,                             
                             "working_directory": os.path.join(os.getcwd(),"work"),
                             'affinity_datacenter_label': "eu-de-south",              
                             'affinity_machine_label': "mymachine" 
                            }
    
    pilotjob = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
         
    compute_data_service = ComputeDataService()
    compute_data_service.add_pilot_compute_service(pilot_compute_service)
    
    # start work unit
    compute_unit_description = {
            "executable": "/bin/date",
            "arguments": [""],
            "number_of_processes": 1,            
            "output": "stdout.txt",
            "error": "stderr.txt",   
            "affinity_datacenter_label": "eu-de-south",              
            "affinity_machine_label": "mymachine" 
    }   
                                                                                                                        
    compute_unit = compute_data_service.submit_compute_unit(compute_unit_description)
    
    compute_data_service.wait()

    compute_data_service.cancel()
    
    
       
'''


application_id = "bigjob"

from pilot.impl.pilotcompute_manager import PilotComputeService
from pilot.impl.pilotcompute_manager import PilotCompute
from pilot.impl.pilotdata_manager import PilotDataService
from pilot.impl.pilotdata_manager import PilotData
from pilot.impl.pilotdata_manager import DataUnit
from pilot.impl.pilot_manager import ComputeDataService
from pilot.impl.pilot_manager_decentral import ComputeDataServiceDecentral 
from pilot.impl.pilot_manager import ComputeUnit 
from pilot.api.api import ComputeDataDescription
from pilot.api.api import PilotError
from pilot.api import State 


class ComputeUnitService():
    
    def __init__(self):
        raise NotImplementedError("Please use ComputeDataService.")


class DataUnitService():
    
    def __init__(self):
        raise NotImplementedError("Please use ComputeDataService.")

