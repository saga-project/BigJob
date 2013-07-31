import sys
import os
import time

from pilot import PilotComputeService, ComputeDataService, State


COORDINATION_URL = "redis://localhost"
NUMBER_CUS=128
NUMBER_SLOTS=16
if __name__ == "__main__":      
    
    pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)

    # create pilot job service and initiate a pilot job
    pilot_compute_description = {
                             "service_url": 'fork://localhost',
                             "number_of_processes": NUMBER_SLOTS,                             
                             "working_directory": os.getcwd(),
                             "number_executor_threads": 16
                            }
    
    pilotjob = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
    
    while pilotjob.get_state()!="Running":
        time.sleep(2) 
        
    start = time.time()
    # start work unit
    compute_unit_description = {
            "executable": "/bin/date",
            "arguments": [""],
            "number_of_processes": 1,            
            "output": "stdout.txt",
            "error": "stderr.txt",   
    }   
    
    cds = ComputeDataService()
    cds.add_pilot_compute_service(pilot_compute_service)
    
    unitservice = pilotjob
    
    for i in range(0,NUMBER_CUS):
        compute_unit = unitservice.submit_compute_unit(compute_unit_description)
    
    
    print("Finished setup. Waiting for scheduling of CU")
    unitservice.wait()
    
    
    runtime=time.time()-start
    print("Number Slots, Number CUs, Runtime, Throughput")
    print("%d,%d,%f,%f"%(NUMBER_SLOTS,NUMBER_CUS,runtime, runtime/NUMBER_CUS))
    
    print("Terminate Pilot Compute and Compute Data Service")
    pilot_compute_service.cancel()
