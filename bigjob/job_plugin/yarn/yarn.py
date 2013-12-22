#!/usr/bin/env python

from bigjob import logger

import httplib2
import os
import uuid
import time
import saga
import subprocess

""" HADOOP/JAVA Configuration"""
JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk1.7.0_40.jdk/Contents/Home"
YARN_HOME="/usr/local/hadoop-2.2.0/"


""" BigJob YARN Client
https://github.com/drelu/BigJob-YARN
"""
BIGJOB_YARN_CLIENT="/Users/luckow/workspace-saga/bigjob/BigJob-YARN/target/BigJob-YARN-0.1-SNAPSHOT-jar-with-dependencies.jar"

BIGJOB_BOOTSTRAP="bootstrap/bigjob-bootstrap.sh"

class YarnStates:
    PROVISIONING="PROVISIONING"
    STAGING="STAGING"
    RUNNING="RUNNING"
    
    
class Service(object):
    """ Plugin for YARN 
    
        Manages endpoint in the form of:
        
            yarn://<resource manager>:<port>    
    """

    def __init__(self, resource_url, pilot_compute_description=None):
        """Constructor"""
        self.resource_url = resource_url
        self.pilot_compute_description =  pilot_compute_description   
    
    def create_job(self, job_description):
        j = Job(job_description, self.resource_url, self.pilot_compute_description)
        return j
            
    
    def __del__(self):
        pass
    
    
    

class Job(object):
    """ Job class for managing a Yarn ApplicationMaster """

    def __init__(self, job_description, resource_manager_url, pilot_compute_description):
        self.job_description = job_description
        self.saga_url = resource_manager_url
        self.pilot_compute_description = pilot_compute_description
        self.id="bigjob-" + str(uuid.uuid1())
        self.yarn_subprocess = None
        
    def run(self):
        java_cmd = os.path.join(JAVA_HOME, "bin", "java")
        bigjob_bootstrap = os.path.join(os.path.dirname(os.path.abspath( __file__ )), 
                                        "..", "..", "..", BIGJOB_BOOTSTRAP)
        cmd = [java_cmd, '-jar', BIGJOB_YARN_CLIENT,  
                         '-jar', BIGJOB_YARN_CLIENT, 
                         '-debug', '-shell_script', bigjob_bootstrap, 
                         '-shell_args',
                          self.pilot_compute_description["coordination_host"], 
                          self.pilot_compute_description["pilot_url"], 
                          self.pilot_compute_description["external_queue"]
                         ]
        
        logger.debug(str(cmd))
        self.yarn_subprocess = subprocess.Popen(cmd)
                            
    
    def get_state(self):
        pass

    
    def cancel(self):
        self.yarn_subprocess.kill()
        
        
    
    def __run_yarn_application(self):
        
        output.wait()

    
    
 
     
if __name__ == "__main__":
    yarn_service = Service("yarn://localhost:8080")
    job = yarn_service.create_job(None)
    job.run()
    