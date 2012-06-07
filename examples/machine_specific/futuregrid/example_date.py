""" Example application demonstrating how to submit a simple 
    /bin/date with PilotJob. 
"""

import os
import time
import sys
import logging
from bigjob import logger
import pdb
logger.setLevel(logging.FATAL)
from pilot import PilotComputeService, ComputeDataService, State

### This is the number of jobs you want to run
NUMBER_JOBS=128

COORDINATION_URL = "advert://localhost/?dbtype=sqlite3"
# COORDINATION_URL = "redis://localhost:6379"

if __name__ == "__main__":

    start_time=time.time()
    pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)
    pilot_compute_description=[]

    pilot_compute_description.append({ "service_url": "ssh://localhost",
                                       "number_of_processes": 8,
                                       "working_directory": "/N/u/pmantha/agent",
                                     })
    pilot_compute_description.append({ "service_url": "pbs://localhost",
                                       "number_of_processes": 8,
                                       "working_directory": "/N/u/pmantha/agent",
                                       "queue":"batch",
                                       "processes_per_node":8,
                                       "walltime":60
                                     })
    pilot_compute_description.append({ "service_url": "pbs+ssh://sierra.futuregrid.org",
                                       "number_of_processes": 8,
                                       "working_directory": "/N/u/pmantha/agent",
                                       "queue":"batch",
                                       "processes_per_node":8,
                                       "walltime":60
                                     })
    pilot_compute_description.append({ "service_url": "pbs+ssh://alamo.futuregrid.org",
                                       "number_of_processes": 8,
                                       "working_directory": "/N/u/pmantha/agent",
                                       "queue":"batch",
                                       "processes_per_node":8,
                                       "walltime":60
                                     })
    pilot_compute_description.append({ "service_url": "pbs+ssh://hotel.futuregrid.org",
                                       "number_of_processes": 8,
                                       "working_directory": "/N/u/pmantha/agent",
                                       "queue":"batch",
                                       "processes_per_node":8,
                                       "walltime":60
                                     })
    pilot_compute_description.append({ "service_url": "pbs+ssh://india.futuregrid.org",
                                       "number_of_processes": 8,
                                       "working_directory": "/N/u/pmantha/agent",
                                       "queue":"batch",
                                       "processes_per_node":8,
                                       "walltime":60
                                     })

    for pcd in pilot_compute_description:
        pj = pilot_compute_service.create_pilot(pilot_compute_description=pcd)

    compute_data_service = ComputeDataService()
    compute_data_service.add_pilot_compute_service(pilot_compute_service)

    # submit work units
    for i in range(NUMBER_JOBS):
        compute_unit_description = {
                "executable": "/bin/date",
                "arguments": [""],
                "number_of_processes": 1,            
                "output": "stdout.txt",
                "error": "stderr.txt"
                }    
        compute_unit = compute_data_service.submit_compute_unit(compute_unit_description)

    logging.debug("Finished setup. Waiting for scheduling of compute units")
    compute_data_service.wait()

    logging.debug("Terminate Pilot Compute and Compute Data Service")
    compute_data_service.cancel()    
    pilot_compute_service.cancel()
    end_time=time.time()
    print "Total time to solution-" + str(round(end_time-start_time,2))
