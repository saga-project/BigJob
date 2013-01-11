import sys
import os
import time

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.getcwd() + "/../")
from pilot import PilotComputeService, ComputeDataServiceDecentral, State

COORDINATION_URL = "redis://localhost:6379"

if __name__ == "__main__":      
    
    pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)

    # create pilot job service and initiate a pilot job
    pilot_compute_description = {
                             "service_url": 'fork://localhost',
                             "number_of_processes": 1,                             
                             "working_directory": os.path.join(os.getcwd(),"work"),
                             'affinity_datacenter_label': "eu-de-south",              
                             'affinity_machine_label': "mymachine" 
                            }
    
    pilotjob = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
    pilotjob2 = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
    
    compute_data_service = ComputeDataServiceDecentral()
    compute_data_service.add_pilot_compute_service(pilot_compute_service)
    
    # start work unit
    compute_unit_description = {
            "executable": "/bin/date",
            "arguments": [""],
            "number_of_processes": 1,            
            "output": "stdout.txt",
            "error": "stderr.txt",
    }    
    
    for i in range(0,4):
        compute_unit = compute_data_service.submit_compute_unit(compute_unit_description)
    
    
    print("Finished setup. Waiting for scheduling of CU")
    compute_data_service.wait()
    
    while compute_unit != State.Done:
        print("Final state check...")
        state_cu = compute_unit.get_state()
        print "PCS State %s" % pilot_compute_service
        print "CU: %s State: %s"%(compute_unit, state_cu)
        if state_cu==State.Done:
            break
        time.sleep(2)  
    
    print("Terminate Pilot Compute and Compute Data Service")
    compute_data_service.cancel()    
    pilot_compute_service.cancel()
