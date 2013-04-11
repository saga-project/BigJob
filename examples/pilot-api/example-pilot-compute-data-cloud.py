import sys
import os
import time
import logging
import uuid
#logging.basicConfig(level=logging.DEBUG)

#sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from pilot import PilotComputeService, PilotDataService, ComputeDataService, State
from bigjob import logger 

COORDINATION_URL = "redis://localhost:6379"

if __name__ == "__main__":      
    
    print COORDINATION_URL
    # create pilot data service (factory for data pilots (physical, distributed storage))
    # and pilot data
    pilot_data_service = PilotDataService(coordination_url=COORDINATION_URL)
    
    ###################################################################################################
    # Pick one of the Pilot Data Descriptions below    
    
    pilot_data_description_aws={
                                "service_url": "s3://pilot-data-" + str(uuid.uuid1()),
                                "size": 100,   
                                #"region" : "", # or "" for DEFAULT/EAST
                                "access_key_id":"AKIAJPGNDJRYIG5LIEUA",
                                "secret_access_key":"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"                                                       
                              }
    
    pilot_data_description_india = {
                                "service_url": "walrus://149.165.146.135/pilot-data-" + str(uuid.uuid1()),
                                #"service_url": "ssh://localhost/tmp/pilot-data-" + str(uuid.uuid1()),
                                "affinity_datacenter_label": "us-east",              
                                "affinity_machine_label": "",
                                "access_key_id":"",
                                "secret_access_key":""
                             }
    
    
    pd = pilot_data_service.create_pilot(pilot_data_description=pilot_data_description_aws)
     
     
    # Create Data Unit Description
    #base_dir = "../data1"
    #url_list = os.listdir(base_dir)
    # make absolute paths
    #absolute_url_list = [os.path.join(base_dir, i) for i in url_list]
    data_unit_description = {
                              "file_urls": [os.path.join(os.getcwd(), "test.txt")],
                              "affinity_datacenter_label": "us-east-1",              
                              "affinity_machine_label": ""
                             }    
      
    # submit pilot data to a pilot store 
    input_data_unit = pd.submit_data_unit(data_unit_description)
    input_data_unit.wait()
    
    logger.info("Data Unit URL: " + input_data_unit.get_url())
    pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)
    
    pilot_compute_description_amazon_west = {
                             "service_url": 'ec2+ssh://aws.amazon.com',
                             "number_of_processes": 1,                             
                             'affinity_datacenter_label': "us-google",              
                             'affinity_machine_label': "", 
                             # cloud specific attributes
                             #"vm_id":"ami-d7f742be",
                             "vm_id": "ami-5c3b1b19",
                             "vm_ssh_username":"ubuntu",
                             "vm_ssh_keyname":"MyKey",
                             "vm_ssh_keyfile":"/Users/luckow/.ssh/id_rsa",
                             "vm_type":"t1.micro",
                             "region" : "us-west-1",
                             "access_key_id":"AKIAJPGNDJRYIG5LIEUA",
                             "secret_access_key":"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"
                           
                            }
    
    
    pilot_compute_description_euca_india = {
                             "service_url": 'euca+ssh://149.165.146.135:8773/services/Eucalyptus',
                             #"service_url": 'fork://localhost',
                             "number_of_processes": 1,
                             'affinity_datacenter_label': "us-east",              
                             'affinity_machine_label': "", 
                             #'working_directory': os.getcwd(),
                             # cloud specific attributes
                             "vm_id":"emi-36913A82",
                             "vm_ssh_username":"root",
                             "vm_ssh_keyname":"luckow",
                             "vm_ssh_keyfile":"/Users/luckow/.ssh/eucakey-india",
                             "vm_type":"c1.xlarge",
                             "access_key_id":"",
                             "secret_access_key":""
                            }
    
    pilotjob = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description_amazon_west)
    
    compute_data_service = ComputeDataService()
    compute_data_service.add_pilot_compute_service(pilot_compute_service)
    compute_data_service.add_pilot_data_service(pilot_data_service)
    
    # create empty data unit for output data
    output_data_unit_description = {
         "file_urls": []              
    }
    output_data_unit = pd.submit_data_unit(output_data_unit_description)
    output_data_unit.wait()
    
    # create compute unit
    compute_unit_description = {
            "executable": "/bin/cat",
            "arguments": ["test.txt"],
            "number_of_processes": 1,
            "output": "stdout.txt",
            "error": "stderr.txt",   
            "input_data": [input_data_unit.get_url()],
            # Put files stdout.txt and stderr.txt into output data unit
            "output_data": [
                            {
                             output_data_unit.get_url(): 
                             ["std*"]
                            }
                           ]    
    }   
    
    compute_unit = compute_data_service.submit_compute_unit(compute_unit_description)
    logger.info("Finished setup of ComputeDataService. Waiting for scheduling of PD")
    compute_data_service.wait()
    
    logger.debug("Output Data Unit: " + str(output_data_unit.list()))
    
    logger.info("Terminate Pilot Compute/Data Service")
    compute_data_service.cancel()
    pilot_data_service.cancel()
    pilot_compute_service.cancel()
