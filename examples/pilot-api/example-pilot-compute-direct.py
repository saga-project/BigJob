import sys
import os
import time
import logging
logging.basicConfig(level=logging.DEBUG)

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from pilot import PilotComputeService, ComputeDataService, ComputeUnit, State

COORDINATION_URL = "advert://localhost/?dbtype=sqlite3"
#COORDINATION_URL = "advert://SAGA:SAGA_client@advert.cct.lsu.edu:8080/?dbtype=postgresql"
#COORDINATION_URL = "redis://localhost:6379"

if __name__ == "__main__":      
    
    pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)

    # create pilot job service and initiate a pilot job
    pilot_compute_description = {
                             "service_url": 'fork://localhost',
                             "number_of_processes": 1,                             
                             "working_directory": "/tmp/pilot-compute/",
                             'affinity_datacenter_label': "eu-de-south",              
                             'affinity_machine_label': "mymachine-1" 
                            }
    
    pilotjob = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
    
    # start compute unit
    compute_unit_description = {
            "executable": "/bin/date",
            "arguments": [""],
            "total_core_count": 1,
            "number_of_processes": 1,
             "output": "stdout.txt",
            "error": "stderr.txt",   
            "affinity_datacenter_label": "eu-de-south",              
            "affinity_machine_label": "mymachine-1" 
    }    
    
    compute_unit = ComputeUnit(compute_unit_description)
    pilotjob.submit_cu(compute_unit)
    logging.debug("Finished setup of PSS and PDS. Waiting for scheduling of PD")
    compute_unit.wait()
    
    
    logging.debug("Terminate Pilot Compute Service")
    pilot_compute_service.cancel()