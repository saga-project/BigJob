#!/usr/bin/env python

from bigjob import logger

import httplib2
import os
import uuid
import time
import saga
import subprocess
import threading
import tempfile

from bigjob.state import Running, New, Failed, Done, Unknown

""" HADOOP/JAVA Configuration"""
JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk1.7.0_40.jdk/Contents/Home"
YARN_HOME="/usr/local/hadoop-2.2.0/"
YARN_EXECUTABLE=os.path.join(YARN_HOME, "bin/yarn")

""" BigJob YARN Client
https://github.com/drelu/BigJob-YARN
"""
#BIGJOB_YARN_CLIENT="/Users/luckow/workspace-saga/bigjob/BigJob-YARN/target/BigJob-YARN-0.1-SNAPSHOT-jar-with-dependencies.jar"
BIGJOB_YARN_CLIENT=os.path.join(os.path.dirname(__file__), "BigJob-YARN-0.1-SNAPSHOT-jar-with-dependencies.jar")
BIGJOB_BOOTSTRAP="bootstrap/bigjob2-bootstrap.sh"



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
        self.id = None
        self.yarn_subprocess = None
        self.state = Unknown
        
        
    def run(self):
#         self.thread = threading.Thread(target=self.__run_yarn_application, args=())
#         self.thread.daemon = True
#         self.thread.start()
        return self.__run_yarn_application()
    
    
    def get_state(self):
        cmd = [YARN_EXECUTABLE, 'application', '-status', self.id]
        self.yarn_subprocess = subprocess.Popen(cmd,
                                                bufsize=0,
                                                stderr=subprocess.PIPE,
                                                stdout=subprocess.PIPE
                                                )
        rc = self.yarn_subprocess.wait()
        while True:
            line = self.yarn_subprocess.stdout.readline()
            if not line:
                break       
            elif line.find("State") >= 0:
                new_state = line[len("State : "):].strip()
                self.state = new_state
                break
        logger.debug("GetStatus YARN job command: " + str(cmd) + " State: " + str(self.state) + " Return code: " + str(rc))
        return self.state        
        
    
    def cancel(self):
        cmd = [YARN_EXECUTABLE, 'application', '-kill', self.id]
        self.yarn_subprocess = subprocess.Popen(cmd)
        rc = self.yarn_subprocess.wait()
        logger.debug("Cancel YARN job command: " + str(cmd) + " Return code: " + str(rc))
        
    
    def __run_yarn_application(self):
        java_cmd = os.path.join(JAVA_HOME, "bin", "java")
        bigjob_bootstrap = os.path.join(os.path.dirname(os.path.abspath( __file__ )), 
                                        "..", "..", "..", BIGJOB_BOOTSTRAP)
        cmd = [java_cmd, '-jar', BIGJOB_YARN_CLIENT,  
                         '-jar', BIGJOB_YARN_CLIENT, 
                         '-debug', 
                         '-service_url', str(self.saga_url),
                         '-shell_script', bigjob_bootstrap, 
                         '-shell_args',
                          self.pilot_compute_description["coordination_host"], 
                          self.pilot_compute_description["pilot_url"], 
                          self.pilot_compute_description["external_queue"]
                         ]
        logger.debug("Submit to YARN with command: " + str(cmd))
        self.yarn_subprocess = subprocess.Popen(cmd,
                                                bufsize=0,
                                                stderr=subprocess.PIPE,
                                                stdout=subprocess.PIPE
                                                )
        while True:
            line = self.yarn_subprocess.stdout.readline()
            if not line:
                break       
            elif line.startswith("ApplicationId"):
                self.id = line[len("ApplicationId:"):].strip()
                break
            
        self.yarn_subprocess.wait()                      
        if self.id !=None:       
            logger.debug("Submission to YARN successfull. Application ID: " + self.id)
            return self.id
        else:
            return None
 
     
if __name__ == "__main__":
    yarn_service = Service("yarn://localhost:8080")
    job = yarn_service.create_job(None)
    job.run()
    