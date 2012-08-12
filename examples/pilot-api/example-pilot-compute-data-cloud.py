import sys
import os
import time
import logging
import uuid
#logging.basicConfig(level=logging.DEBUG)

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from pilot import PilotComputeService, PilotDataService, ComputeDataService, State


COORDINATION_URL = "redis://localhost:6379"

if __name__ == "__main__":      
    
    
    # create pilot data service (factory for data pilots (physical, distributed storage))
    # and pilot data
    pilot_data_service = PilotDataService(coordination_url=COORDINATION_URL)
    pilot_data_description={
                                "service_url": "gs://pilot-data-" + str(uuid.uuid1()),
                                "size": 100,   
                                "affinity_datacenter_label": "us-google",              
                                "affinity_machine_label": ""                              
                             }
    pd = pilot_data_service.create_pilot(pilot_data_description=pilot_data_description)
     
     
    # Create Data Unit Description
    #base_dir = "../data1"
    #url_list = os.listdir(base_dir)
    # make absolute paths
    #absolute_url_list = [os.path.join(base_dir, i) for i in url_list]
    data_unit_description = {
                              "file_urls": [os.path.join(os.getcwd(), "test.txt")],
                              "affinity_datacenter_label": "us-google",              
                              "affinity_machine_label": ""
                             }    
      
    # submit pilot data to a pilot store 
    data_unit = pd.submit_data_unit(data_unit_description)
    data_unit.wait()
    print("Data Unit URL: " + data_unit.get_url())
    
    pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)

    # create pilot job service and initiate a pilot job
    pilot_compute_description = {
                             #"service_url": 'ec2+ssh://aws.amazon.com',
                             #"service_url": 'gce+ssh://api.google.com',
                             "service_url": 'fork://localhost',
                             "number_of_processes": 1,                             
                             'affinity_datacenter_label': "us-google",              
                             'affinity_machine_label': "" 
                            }
    
    pilotjob = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
    
    compute_data_service = ComputeDataService()
    compute_data_service.add_pilot_compute_service(pilot_compute_service)
    compute_data_service.add_pilot_data_service(pilot_data_service)
    
    
    # start work unit
    compute_unit_description = {
            "executable": "/bin/cat",
            "arguments": ["test.txt"],
            "number_of_processes": 1,
            "output": "stdout.txt",
            "error": "stderr.txt",   
            "input_data": [data_unit.get_url()],
            "output_data": []
    }    
    
    compute_unit = compute_data_service.submit_compute_unit(compute_unit_description)
    logging.debug("Finished setup of ComputeDataService. Waiting for scheduling of PD")
    compute_data_service.wait()
    
    
    logging.debug("Terminate Pilot Compute/Data Service")
    compute_data_service.cancel()
    pilot_data_service.cancel()
    pilot_compute_service.cancel()