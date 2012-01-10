import sys
import os
import time
import logging
logging.basicConfig(level=logging.DEBUG)

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.getcwd() + "/../")
from pstar import PilotJobService, WorkDataService, State

if __name__ == "__main__":      
    
    pilot_job_service = PilotJobService()

    # create pilot job service and initiate a pilot job
    pilot_job_description = {
                             "service_url": 'fork://localhost',
                             "number_of_processes": 1,                             
                             "working_directory": "/tmp/pilotjob/",
                             'affinity_datacenter_label': "eu-de-south",              
                             'affinity_machine_label': "mymachine" 
                            }
    
    pilotjob = pilot_job_service.create_pilotjob(pilot_job_description=pilot_job_description)
    pilotjob2 = pilot_job_service.create_pilotjob(pilot_job_description=pilot_job_description)
         
    work_data_service = WorkDataService()
    work_data_service.add_pilot_job_service(pilot_job_service)
    
    # start work unit
    work_unit_description = {
            "executable": "/bin/date",
            "arguments": [],
            "total_core_count": 1,
            "number_of_processes": 1,
            "working_directory": os.getcwd(),
            "output": "stdout.txt",
            "error": "stderr.txt",   
            "affinity_datacenter_label": "eu-de-south",              
            "affinity_machine_label": "mymachine" 
    }    
    work_unit = work_data_service.submit_work_unit(work_unit_description)
    
    logging.debug("Finished setup. Waiting for scheduling of PD")
    while work_unit != State.Done:
        logging.debug("Check state")
        
        state_wu = work_unit.get_state()
        print "PJS State %s" % pilot_job_service
        print "WU: %s State: %s"%(work_unit, state_wu)
        if state_wu==State.Done:
            break
        time.sleep(2)  
    
    logging.debug("Terminate Pilot Data/Store Service")
    work_data_service.cancel()    
    pilot_job_service.cancel()