import sys
import os
import time
import logging
import uuid
#logging.basicConfig(level=logging.DEBUG)

#sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from pilot import PilotComputeService, PilotDataService, ComputeDataService, State
from bigjob import logger 

#COORDINATION_URL = "redis://localhost:6379"

if __name__ == "__main__":      
     
    pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)

    # create pilot job service and initiate a pilot job
    pilot_compute_description_amazon = {
                             "service_url": 'ec2+ssh://aws.amazon.com',
                             #"service_url": 'euca+ssh://198.202.120.90/',
                             #"service_url": 'gce+ssh://api.google.com',
                             #"service_url": 'fork://localhost',
                             "number_of_processes": 1,                             
                             'affinity_datacenter_label': "us-google",              
                             'affinity_machine_label': "", 
                             # cloud specific attributes
                             "vm_id":"ami-d7f742be",
                             "vm_ssh_username":"ubuntu",
                             "vm_ssh_keyname":"MyKey",
                             "vm_ssh_keyfile":"/Users/luckow/.ssh/id_rsa",
                             "vm_type":"t1.micro",
                             "access_key_id":"x",
                             "secret_access_key":"x"
                            }
    
    pilot_compute_description_amazon_west = {
                             "service_url": 'ec2+ssh://aws.amazon.com',
                             "number_of_processes": 1,                             
                             'affinity_datacenter_label': "us-google",              
                             'affinity_machine_label': "", 
                             # cloud specific attributes
                             #"vm_id":"ami-d7f742be",
                             "vm_id": "ami-7e37173b",
                             "vm_ssh_username":"ubuntu",
                             "vm_ssh_keyname":"MyKey",
                             "vm_ssh_keyfile":"/Users/luckow/.ssh/id_rsa",
                             "vm_type":"t1.micro",
                             "region" : "us-west-1",
                             "access_key_id":"x",
                             "secret_access_key":"x"
                           
                            }
    
    pilot_compute_description_euca_sierra = {
                             #"service_url": 'ec2+ssh://aws.amazon.com',
                             "service_url": 'euca+ssh://198.202.120.90/',
                             #"service_url": 'gce+ssh://api.google.com',
                             #"service_url": 'fork://localhost',
                             "number_of_processes": 1,                             
                             'affinity_datacenter_label': "us-west",              
                             'affinity_machine_label': "", 
                             # cloud specific attributes
                             "vm_id":"ami-d7f742be",
                             "vm_ssh_username":"ubuntu",
                             "vm_ssh_keyname":"MyKey",
                             "vm_ssh_keyfile":"/Users/luckow/.ssh/id_rsa",
                             "vm_type":"c1.xlarge",
                             "access_key_id":"OQTYSHQ9I6ACEXDTKTOGG",
                             "secret_access_key":"STgYQPTyh89EWKp7lFtd77A4O2aghXjx0owqkNHm"
                            }
    
    pilot_compute_description_euca_india = {
                             "service_url": 'euca+ssh://149.165.146.135:8773/services/Eucalyptus',
                             "number_of_processes": 1,                             
                             'affinity_datacenter_label': "us-east",              
                             'affinity_machine_label': "", 
                             # cloud specific attributes
                             "vm_id":"emi-9DCC3DFA",
                             "vm_ssh_username":"root",
                             "vm_ssh_keyname":"luckow",
                             "vm_ssh_keyfile":"/Users/luckow/.ssh/eucakey-india",
                             "vm_type":"c1.xlarge",
                             "access_key_id":"8MCXRAMXMHDYKWNKXZ8WF",
                             "secret_access_key":"YrcUqSw2Arxshrh3ZtenkxerWwCWdMTKvZYoLPAo"
                            }
    
    pilot_compute_description_openstack_india = {
                             "service_url": 'nova+ssh://149.165.146.50:8773/services/Cloud',
                             "number_of_processes": 1,                             
                             'affinity_datacenter_label': "us-east",              
                             'affinity_machine_label': "", 
                             # cloud specific attributes
                             "vm_id":"ami-00000046",
                             "vm_ssh_username":"root",
                             "vm_ssh_keyname":"novakey-india",
                             "vm_ssh_keyfile":"/Users/luckow/.ssh/novakey-india",
                             "vm_type":"m1.xlarge",
                             "access_key_id":"f9716a49c92a4a4cbedb6aba5e78d682",
                             "secret_access_key":"bcdff54b7fe94d63b4412c762e823a84"
                            }
    
    pilotjob = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description_amazon_west)
    
    compute_data_service = ComputeDataService()
    compute_data_service.add_pilot_compute_service(pilot_compute_service)
    
    
    # start work unit
    compute_unit_description = {
            "executable": "/bin/date",
            "arguments": [],
            "number_of_processes": 1,
            "output": "stdout.txt",
            "error": "stderr.txt",   
            "input_data": [],
            "output_data": []
    }    
    
    compute_unit = compute_data_service.submit_compute_unit(compute_unit_description)
    logging.info("Finished setup of ComputeDataService. Waiting for scheduling of PD")
    compute_data_service.wait()
    
    
    logging.info("Terminate Pilot Compute/Data Service")
    compute_data_service.cancel()
    pilot_compute_service.cancel()
