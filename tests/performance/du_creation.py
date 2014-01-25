import sys
import os
import time
import logging
import time
#logging.basicConfig(level=logging.DEBUG)

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from pilot import PilotComputeService, PilotDataService, ComputeDataService, State


COORDINATION_URL = "redis://localhost:6379"

if __name__ == "__main__":      
    
    pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)

    # create pilot job service and initiate a pilot job
    pilot_compute_description = {
                             "service_url": 'fork://localhost',
                             "number_of_processes": 1,                             
                             "working_directory": os.getcwd() + "/work/",
                            }
    
    pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
    
    
    # create pilot data service (factory for data pilots (physical, distributed storage))
    # and pilot data
    pilot_data_service = PilotDataService(coordination_url=COORDINATION_URL)
    pilot_data_description={
                                "service_url": "ssh://localhost/tmp/pilot-data/",
                                "size": 100,   
                                #"userkey":"/Users/luckow/.ssh/rsa_osg",                             
                             }
    
    pilot_data_service.create_pilot(pilot_data_description=pilot_data_description)
     
    compute_data_service = ComputeDataService()
    compute_data_service.add_pilot_compute_service(pilot_compute_service)
    compute_data_service.add_pilot_data_service(pilot_data_service)
    
    # Create Data Unit Description
    #base_dir = "../data1"
    #url_list = os.listdir(base_dir)
    # make absolute paths
    #absolute_url_list = [os.path.join(base_dir, i) for i in url_list]
    data_unit_description = {
                              "file_urls": ["/Users/luckow/workspace-saga/github-projects/kmeans/python/rand/random_data_0.3GB.csv"]
                             }    
      
    start = time.time() 
    for i in range(0,128):
        data_unit = compute_data_service.submit_data_unit(data_unit_description)
        logging.debug("Submitted Data Unit: " + data_unit.get_url())
        logging.debug("Pilot Data URL: %s Description: \n%s"%(data_unit, str(pilot_data_description)))
    
    compute_data_service.wait()
    print "Runtime: " + str(time.time()-start)
    logging.debug("Terminate Pilot Compute/Data Service")
    compute_data_service.cancel()
    pilot_data_service.cancel()
    pilot_compute_service.cancel()
