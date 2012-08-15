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
    data_unit_description1 = {
                               "file_urls":absolute_url_list,
                               'affinity_datacenter_label': "eu-de-south",              
                               'affinity_machine_label': "mymachine-1" 
                               }
    logging.debug("Pilot Data Description 1: \n%s"%str(data_unit_description1))
    
    
    # What files? Create Pilot Data Description using remote SSH URLs
    # make remotete paths
    remote_url_list = ["ssh://localhost"+os.path.join(base_dir, i) for i in url_list]
    data_unit_description2 = {
                               "file_urls":remote_url_list,
                               'affinity_datacenter_label': "eu-de-south",              
                               'affinity_machine_label': "mymachine-2" 
                              }
    
    logging.debug("Pilot Data Description 2: \n%s"%str(data_unit_description2))
        
    
    
    # create pilot data service (factory for pilot stores (physical, distributed storage))
    pilot_data_service = PilotDataService(coordination_url=COORDINATION_URL)
    ps1 = pilot_data_service.create_pilot({
                                'service_url': "ssh://localhost/tmp/pilotdata-1/",
                                'size':100,
                               'affinity_datacenter_label': "eu-de-south",              
                               'affinity_machine_label': "mymachine-1"
                                })
    
    ps2 = pilot_data_service.create_pilot({
                                'service_url': "ssh://localhost/tmp/pilotdata-2/",
                                'size':100,
                               'affinity_datacenter_label': "eu-de-south",              
                               'affinity_machine_label': "mymachine-2"
                                })
    
    
      # create pilot data service
    compute_data_service = ComputeDataService()
  
    # add resources to pilot data service    
    compute_data_service.add_pilot_data_service(pilot_data_service) 
    
    ###########################################################################
    # DU1 should only be scheduled to machine 1
    # DU2 should only be scheduled to machine 2
    du1 = compute_data_service.submit_data_unit(data_unit_description1)
    du2 = compute_data_service.submit_data_unit(data_unit_description2)
    
    logging.debug("Finished setup of Pilot Data and Compute Data Service. Waiting for scheduling of Data Units")
    
    compute_data_service.wait()
    logging.debug("DU scheduled: " + du1.url)
    logging.debug("Export files of PD")
    du1.export("/tmp/pilot-data-export/")
    #du2.export("ssh://hotel.futuregrid.org/N/u/luckow/pilot-store-export/")
        
    print "***************************************************************"
    print "To reconnect to Data Unit 1 use the following URL: %s"%du1.url
    print "Run:\n\n " + sys.executable + " example-pilot-data-reconnect.py %s"%du1.url
    print "\n\n******************* SLEEPING *********************************"
    #time.sleep(1200)
    
    logging.debug("Terminate Pilot Data/Compute Data Service")
    compute_data_service.cancel()
    pilot_data_service.cancel()
