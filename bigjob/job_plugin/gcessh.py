#!/usr/bin/env python

from bigjob import logger
from apiclient.discovery import build
from oauth2client.file import Storage
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.tools import run
import httplib2
import os
import uuid
import time

import bliss.saga as saga

"""
AN OAUTH2 Client Id must be created at the Google API console at:

https://code.google.com/apis/console/

=> API Access

More information with respect to OAUTH: https://developers.google.com/compute/docs/api/how-tos/authorization
"""
OAUTH2_CLIENT_ID='1004462711324-55akehip32m59u6omdfrt9s8u8ehb0hm.apps.googleusercontent.com'
OAUTH2_CLIENT_SECRET='EIMML1W7anu0XijVghws0DY-'

GCE_PROJECT_ID='bigjob-pilot'

"""
Google Compute Engine currently provides a default image with Ubuntu 12.04

To use BJ, a custom image containing gcc and build essentials needs to be
created.

$ apt-get update
$ apt-get install gcc python-all-dev 

"""
GCE_IMAGE_URL="https://www.googleapis.com/compute/v1beta12/projects/bigjob-pilot/images/bigjob-image"

class gce_states:
    PROVISIONING="PROVISIONING"
    STAGING="STAGING"
    RUNNING="RUNNING"
    
    
class Service(object):
    """ Plugin for Google Compute Engine 
    
        Manages endpoint in the form of:
        
            gce+ssh://api.google.com    
    """

    def __init__(self, resource_url, pilot_compute_description):
        """Constructor"""
        self.resource_url = resource_url
        self.pilot_compute_description =  pilot_compute_description   
    
    def create_job(self, job_description):
        j = Job(job_description, self.resource_url, self.pilot_compute_description)
        return j
            
    
    def __del__(self):
        pass
    
    
    

class Job(object):


    def __init__(self, job_description, saga_url, pilot_compute_description):
        
        self.job_description = job_description
        self.saga_url = saga_url
        self.pilot_compute_description = pilot_compute_description
        self.image_url = GCE_IMAGE_URL
        if self.pilot_compute_description.has_key("vm_id"):
            self.image_url = self.pilot_compute_description["vm_id"]
            
        self.machine_type = "https://www.googleapis.com/compute/v1beta12/projects/bigjob-pilot/machine-types/n1-standard-1"
        if self.pilot_compute_description.has_key("vm_type"):
            self.machine_type = self.pilot_compute_description["vm_type"]
        
        self.location = "https://www.googleapis.com/compute/v1beta12/projects/bigjob-pilot/zones/us-east1-a"
        if self.pilot_compute_description.has_key("vm_location"):
            self.location = self.pilot_compute_description["vm_location"]
       
            
        self.id="bigjob-" + str(uuid.uuid1())
        self.network_ip=None
        
        # Do OAUTH authentication
        storage = Storage('gce.dat')
        self.credentials = storage.get()
        if self.credentials is None or self.credentials.invalid == True:
            flow = OAuth2WebServerFlow(
                                       client_id=OAUTH2_CLIENT_ID,
                                       client_secret=OAUTH2_CLIENT_SECRET,
                                       scope='https://www.googleapis.com/auth/compute',
                                       user_agent='bigjob-client/1.0')

            self.credentials = run(flow, storage)

        
        
    def run(self):
        request_dict = {
          "kind": "compute#instance",
          "disks": [
                    {
                     "kind": "compute#instanceDisk",
                     "type": "PERSISTENT",
                     "mode": "READ",
                     "deviceName": "reference-genome",
                     "source": "https://www.googleapis.com/compute/v1beta12/projects/bigjob-pilot/disks/reference-genome"
                     }
          ],
          "networkInterfaces": [
            {
              "kind": "compute#instanceNetworkInterface",
              "accessConfigs": [
                {
                  "name": "External NAT",
                  "type": "ONE_TO_ONE_NAT"
                }
              ],
              "network": "https://www.googleapis.com/compute/v1beta12/projects/bigjob-pilot/networks/default"
            }
          ],
          "serviceAccounts": [
                              {
                               "kind": "compute#serviceAccount",
                               "email": "default",
                               "scopes": [
                                          "https://www.googleapis.com/auth/userinfo.email",
                                          "https://www.googleapis.com/auth/compute",
                                          "https://www.googleapis.com/auth/devstorage.full_control"
                                          ]
                               }
                              ],         
          #"zone": "https://www.googleapis.com/compute/v1beta12/projects/bigjob-pilot/zones/us-east1-a",
          "zone": self.location,
          #"machineType": "https://www.googleapis.com/compute/v1beta12/projects/bigjob-pilot/machine-types/n1-standard-1",
          "machineType": self.machine_type,
          "name": self.id,
          "image": self.image_url       
        }
         
        http = httplib2.Http()
        http = self.credentials.authorize(http)
        gce = build("compute", "v1beta12", http=http)
        #result = gce.instances().get(instance="bigjob-pilot", project="bigjob-pilot").execute()
        gce.instances().insert(project=GCE_PROJECT_ID, body=request_dict).execute()
        
        time.sleep(15) # wait for startup
        #wait for compute instance to become active
        self.wait_for_running()
        
        # spawn BJ agent via SSH
        compute_instance_details = self.__get_instance_resource()
        logger.debug("Compute Instance Details: " + str(compute_instance_details))
        self.network_ip = compute_instance_details["networkInterfaces"][0]["accessConfigs"][0]['natIP']
        url = "ssh://" + str(self.network_ip)
        logger.debug("Connect to: %s"%(url))
        js = saga.job.Service(url)
        
        # Submit job
        ctx = saga.Context()
        ctx.type = saga.Context.SSH
        ctx.userid  = self.pilot_compute_description["vm_ssh_username"]
        ctx.userkey = self.pilot_compute_description["vm_ssh_keyfile"]
        js.session.contexts = [ctx]


        job = js.create_job(self.job_description)
        print "Submit pilot job to: " + str(url)
        
        TRIAL_MAX=15
        trials=0
        while trials < TRIAL_MAX:
            try:
                logger.debug("Attempt: %d, submit pilot job to: %s "%(trials,str(url)))
                job.run()
                break
            except:
                trials = trials + 1 
                time.sleep(10)
                if trials == TRIAL_MAX:
                    raise Exception("Submission of agent failed.") 
                
        logger.debug("Job State : %s" % (job.get_state())) 
        
      
        
        print "Job State : %s" % (job.get_state())
       

    def wait_for_running(self):
        while self.get_state()!=gce_states.RUNNING:
            time.sleep(5)
        
    
    def get_state(self):
        result=self.__get_instance_resource()
        return result["status"]
    
    
    def cancel(self):
        http = httplib2.Http()
        http = self.credentials.authorize(http)
        gce = build("compute", "v1beta12", http=http)
        gce.instances().delete(project=GCE_PROJECT_ID, instance=self.id).execute()
        
    
    def __get_instance_resource(self):
        http = httplib2.Http()
        http = self.credentials.authorize(http)
        gce = build("compute", "v1beta12", http=http)
        result = gce.instances().get(project=GCE_PROJECT_ID, instance=self.id).execute()
        return result
    
    
 
     
if __name__ == "__main__":
    gce_service = Service("gce+ssh://api.google.com")
    j = gce_service.create_job(job_description)
    
    gce.run()
    print gce.get_state()
