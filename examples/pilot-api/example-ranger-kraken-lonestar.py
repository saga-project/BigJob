import sys
import os
import time
import logging
from pilot import PilotComputeService, ComputeDataService, State
logging.basicConfig(level=logging.WARNING)

COORDINATION_URL = "redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"

if __name__ == "__main__":      
    
    pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)
    pilot_compute_description=[]

    # create pilot job service and initiate a pilot job
    pilot_compute_description.append({ "service_url": 'sge-ssh://pmantha@login1.ls4.tacc.utexas.edu',
                                  "number_of_processes":24,                             
                                  "walltime":10,
				  "processes_per_node":12,
                                  "queue":"normal", 
                                  "allocation":"TG-MCB090174",
                                  "working_directory": "/home1/01539/pmantha/agent",
                                })
    
    pilot_compute_description.append({
                             "service_url": 'fork://localhost',
                             "number_of_processes": 1,                             
                             "working_directory": os.path.join(os.getcwd(),"work"),
                            })

    pilot_compute_description.append({ "service_url": 'sge-ssh://pmantha@login3.ranger.tacc.utexas.edu',
                                  "number_of_processes":32,
                                  "walltime":10,
                                  "processes_per_node":16,
                                  "queue":"normal",
                                  "allocation":"TG-MCB090174",
                                  "working_directory": "/share/home/01539/pmantha/agent",
                                })

    pilot_compute_description.append({ "service_url": 'pbs-ssh://pmantha@kraken-gsi.nics.teragrid.org',
                                  "number_of_processes":12,
                                  "walltime":10,
                                  "processes_per_node":1,
                                  "queue":"small",
                                  "allocation":"TG-MCB090174",
                                  "working_directory": "/lustre/scratch/pmantha/agent/",
                                })
    for pcd in pilot_compute_description:
        pilotjob = pilot_compute_service.create_pilot(pilot_compute_description=pcd)
         
    compute_data_service = ComputeDataService()
    compute_data_service.add_pilot_compute_service(pilot_compute_service)
    
    # start work unit
    compute_unit_description = {
            "executable": "/bin/date",
            "arguments": [""],
            "total_core_count": 1,
            "number_of_processes": 1,            
            "output": "stdout.txt",
            "error": "stderr.txt"
    }
    
    for i in range(0,20):
        compute_unit = compute_data_service.submit_compute_unit(compute_unit_description)
    
    
    logging.debug("Finished setup. Waiting for scheduling of CU")
    compute_data_service.wait()

    
    logging.debug("Terminate Pilot Compute and Compute Data Service")
    compute_data_service.cancel()    
    pilot_compute_service.cancel()
