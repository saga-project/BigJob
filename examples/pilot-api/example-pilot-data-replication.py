import sys
import os
import time
import logging
logging.basicConfig(level=logging.DEBUG)

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from pilot import PilotComputeService, PilotDataService, ComputeDataService, State

COORDINATION_URL = "redis://localhost:6379"

if __name__ == "__main__":        
    
    # What files? Create Pilot Data Description using absolute URLs
    base_dir = "/Users/luckow/workspace-saga/applications/pilot-store/test/data1"
    url_list = os.listdir(base_dir)
    # make absolute paths
    absolute_url_list = [os.path.join(base_dir, i) for i in url_list]
    data_unit_description = {
                               "file_urls":absolute_url_list,
                               "number_of_replicas": 2
                             }
    logging.debug("Pilot Data Description: \n%s"%str(data_unit_description))
    
    
        
    
    
    # create pilot data service (factory for pilot stores (physical, distributed storage))
    pilot_data_service = PilotDataService(coordination_url=COORDINATION_URL)
    pd1 = pilot_data_service.create_pilot({
                                'service_url': "ssh://localhost/tmp/pilotdata-1/",
                                'size':100,
                               'affinity_datacenter_label': "eu-de-south",              
                               'affinity_machine_label': "mymachine-1"
                                })
    
    pd2 = pilot_data_service.create_pilot({
                                'service_url': "ssh://localhost/tmp/pilotdata-2/",
                                'size':100,
                               'affinity_datacenter_label': "eu-de-south",              
                               'affinity_machine_label': "mymachine-2"
                                })
    
    
    du = pd1.submit_data_unit(data_unit_description)
    du.wait()
    # replicate data unit to pilot data 2
    pd2.submit_data_unit(data_unit=du)
    du.wait()
    
    pd_list = du.list_pilot_data()
        
    logging.debug("PDs: " + str(pd_list))
    
    logging.debug("Terminate Pilot Data/Compute Data Service")
    pilot_data_service.cancel()
