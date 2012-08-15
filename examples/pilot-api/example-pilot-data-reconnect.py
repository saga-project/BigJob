import sys
import os
import time
import logging
import json

logging.basicConfig(level=logging.DEBUG)

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from pilot import PilotDataService, ComputeDataService, DataUnit, State

COORDINATION_URL = "redis://localhost:6379"

if __name__ == "__main__":        
    
    if len(sys.argv)==2:
        reconnect_url=sys.argv[1]
    else:
        print "Usage: " + sys.executable + " " + __file__ + " <Data Unit URL to Reconnect to>"
        sys.exit(-1)
        
    # create pilot data service (factory for pilot stores (physical, distributed storage))
    pilot_data_service = PilotDataService(coordination_url=COORDINATION_URL)
    pd_new = pilot_data_service.create_pilot({
                                'service_url': "ssh://localhost/tmp/pilotdata-reconnect/",
                                'size':100,
                               'affinity_datacenter_label': "eu-de-south",              
                               'affinity_machine_label': "mymachine-1"
                                })
    
    
    logging.debug("Pilot Data URL: %s"%pilot_data_service.url)
    
    
    ###########################################################################
    # PD should only be scheduled to machine 1    
    logging.debug("Connect to PD URL: %s"%reconnect_url)
    pd = DataUnit(du_url=reconnect_url)
    
    
    # Move PD to another pilot store    
    pd.add_pilot_data(pd_new)
    
    #time.sleep(120)
    #pilot_data_service.cancel()