import sys
import os
import time
import logging
import uuid
import traceback
#logging.basicConfig(level=logging.DEBUG)

#sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from pilot import PilotComputeService, PilotDataService, ComputeDataService, State
from bigjob import logger 

COORDINATION_URL = "redis://192.168.0.7:6379"

if __name__ == "__main__":
          
    pilotjob = None
    
    try: 
        pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)
    
        # create pilot job service and initiate a pilot job
        pilot_compute_description = {
                                    "service_url": 'yarn://localhost:8032?fs=hdfs://localhost:9000',
                                    "number_of_processes": 1                            
                                    }
        
        pilotjob = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
        logger.debug("Pilot Job State: " + str(pilotjob.get_state()))
        pilotjob.wait() 
        logger.debug("Pilot Job State: " + str(pilotjob.get_state()))
            
        # start compute unit
        compute_unit_description = {
                "executable": "/bin/date",
                "arguments": [],
                "number_of_processes": 1,
                "output": "stdout.txt",
                "error": "stderr.txt"
        }    
        
        compute_unit = pilotjob.submit_compute_unit(compute_unit_description)
        logging.info("Finished setup of ComputeDataService. Waiting for scheduling of PD")
        compute_unit.wait()
        
        logging.info("Terminate Pilot Compute/Data Service")
        pilotjob.cancel()
        logger.debug("Pilot Job State: " + str(pilotjob.get_state()))

    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "*** print_tb:"
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        print "*** print_exception:"
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)
    
    finally:
        if pilotjob!=None:
            pilotjob.cancel()
