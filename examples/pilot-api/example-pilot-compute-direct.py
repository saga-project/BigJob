import sys
import os
import time
import logging
logging.basicConfig(level=logging.DEBUG)

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from pilot import PilotComputeService, ComputeDataService, State

COORDINATION_URL = "redis://localhost:6379"

if __name__ == "__main__":      
    
    pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)

    # create pilot job service and initiate a pilot job
    pilot_compute_description = {
                             "service_url": 'fork://localhost',
                             "number_of_processes": 1,                             
                             #"working_directory": os.path.join(os.getcwd(), "agent"),
                             #"file_transfer": ["ssh://" + os.path.dirname(os.path.abspath(__file__)) 
                             #                  + "/../test.txt > BIGJOB_WORK_DIR"]
                            }
    
    pilotjob = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
    
    # start compute unit
    compute_unit_description = {
            "executable": "/bin/echo",
            "arguments": ["$TBD_DIR"],
            "number_of_processes": 1,
            "environment": ["TBD_DIR=/tmp"],
            "output": "stdout.txt",
            "error": "stderr.txt",   
            "file_transfer": ["ssh://" + os.path.dirname(os.path.abspath(__file__)) 
                                + "/../test.txt > BIGJOB_WORK_DIR"]
    }    
   
    compute_unit = pilotjob.submit_compute_unit(compute_unit_description)
    logging.debug("Finished submission. Waiting for completion of CU")
    compute_unit.wait()
    
    print str(compute_unit.get_details())
    
    logging.debug("Terminate Pilot Compute Service")
    pilot_compute_service.cancel()
