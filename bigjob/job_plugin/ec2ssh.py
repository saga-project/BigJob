#!/usr/bin/env python

from bigjob import logger
import os
import uuid
import time
import traceback
import sys
from boto.ec2.connection import EC2Connection
from boto.ec2.regioninfo import RegionInfo
import boto.ec2

import saga

###############################################################################
# EC2 General
PLACEMENT_GROUP=None
SECURITY_GROUP="default"

# VM/Image specific configurations
# Recommendation Ubuntu > 1104
# apt-get install gcc python-all-dev git subversion vim
# EC2_AMI_ID="ami-c7943cae" #  ami-82fa58eb official Amazon Ubuntu 12.04 LTS (requires dev tools installation)
# EC2_AMI_ID="ami-d7f742be"
# EC2_USERNAME="ubuntu"
# EC2_KEYNAME="lsu-keypair"
# EC2_KEYNAME="MyKey"

# Authentication
# Please use ~/.boto file to configure your security credentials (if possible)
# see http://boto.readthedocs.org/en/latest/boto_config_tut.html
# 
# [Credentials]
# aws_access_key_id = <your access key>
# aws_secret_access_key = <your secret key>
#
# Alternatively you can use these two variables
AWS_ACCESS_KEY_ID=None
AWS_SECRET_ACCESS_KEY=None




class State:
    UNKNOWN="unknown"
    PENDING="pending"
    RUNNING="running"
    
    
class Service(object):
    """ Plugin for Amazon EC2 and EUCA
    
        Manages endpoint in the form of:
        
            ec2+ssh://<EC2 Endpoint>
            euca+ssh://<EUCA Endpoint>
    """

    def __init__(self, resource_url, pilot_compute_description=None):
        """Constructor"""
        self.resource_url = resource_url
        self.pilot_compute_description = pilot_compute_description
            
    
    def create_job(self, job_description):
        j = Job(job_description, self.resource_url, self.pilot_compute_description)
        return j
            
    
    def __del__(self):
        pass
    
    
    

class Job(object):
    """ Plugin for Amazon EC2 
    
        Starts VM and executes BJ agent on this VM    
        
        
        Eucalyptus on FutureGrid uses a self-signed certificate, which 1) needs to be added to boto configuration
        or 2) certificate validation needs to be disabled.
    """

    def __init__(self, job_description, resource_url, pilot_compute_description):
        
        self.job_description = job_description
        logger.debug("URL: " + str(resource_url) + " Type: " + str(type(resource_url)))
        self.resource_url = saga.Url(str(resource_url))
        self.pilot_compute_description = pilot_compute_description
        
        self.id="bigjob-" + str(uuid.uuid1())
        self.network_ip=None
        
        self.ec2_conn=None
        
        if self.resource_url.scheme == "euca+ssh" or self.resource_url.scheme == "nova+ssh":
            host = self.resource_url.host
            path = "/services/Eucalyptus"
            if self.resource_url.path!=None: 
                path = self.resource_url.path
            port = 8773
            if self.resource_url.port != None:
                port = self.resource_url.port
            region = None
            logger.debug("Host: %s, Path: %s, Port: %d"%(host, path, port))
            if self.resource_url.scheme == "euca+ssh":
                region = RegionInfo(name="eucalyptus", endpoint=host)
            elif self.resource_url.scheme == "nova+ssh":
                region = RegionInfo(name="openstack", endpoint=host)
            logger.debug("Access Key: %s Secret: %s"%(self.pilot_compute_description["access_key_id"],
                                                      self.pilot_compute_description["secret_access_key"]))
            self.ec2_conn = EC2Connection(aws_access_key_id=self.pilot_compute_description["access_key_id"],
                                          aws_secret_access_key=self.pilot_compute_description["secret_access_key"], 
                                          region=region,
                                          is_secure=False,
                                          port=port,
                                          path=path)
        else:
            aws_region = None
            if self.pilot_compute_description.has_key("region"):
                region =  self.pilot_compute_description["region"]
                logger.debug("Connect to region: %s"%(str(region)))
                aws_region = boto.ec2.get_region(region,  
                                                 aws_access_key_id=self.pilot_compute_description["access_key_id"], 
                                                 aws_secret_access_key=self.pilot_compute_description["secret_access_key"]
                                                 )

                
            self.ec2_conn = EC2Connection(aws_access_key_id=self.pilot_compute_description["access_key_id"], 
                                          aws_secret_access_key=self.pilot_compute_description["secret_access_key"],
                                          region = aws_region)
            
        self.instance = None
        
        
    def run(self):
        """ Start VM and start BJ agent via SSH on VM """
        
        """ Map fields of Pilot description to EC2 API
            { "vm_id":"ami-d7f742be",
              "vm_ssh_username":"ubuntu",
              "vm_ssh_keyname":"MyKey",
              "vm_ssh_keyfile":"<path>",
              "vm_type":"t1.micro",
              "access_key_id":"xxx",
              "secret_access_key":"xxx"
            }
        """    
            
        reservation = self.ec2_conn.run_instances(self.pilot_compute_description["vm_id"],
                                    key_name=self.pilot_compute_description["vm_ssh_keyname"],
                                    instance_type=self.pilot_compute_description["vm_type"],
                                    security_groups=[SECURITY_GROUP])
                
        self.instance = reservation.instances[0]
        self.instance_id = self.instance.id
        logger.debug("Started EC2/Eucalyptus/Nova instance: %s"%self.instance_id)
        time.sleep(5)
        self.wait_for_running()
        
        if self.resource_url.scheme != "euca+ssh" and self.resource_url.scheme != "nova+ssh":
            self.ec2_conn.create_tags([self.instance_id], {"Name": self.id})
      
        
        self.network_ip = self.instance.ip_address 
        url = "ssh://" + str(self.network_ip)
        logger.debug("Connect to: %s"%(url))

        
        # Submit job
        ctx = saga.Context("SSH")
        #ctx.type = saga.Context.SSH
        ctx.user_id = self.pilot_compute_description["vm_ssh_username"]
        ctx.user_key = self.pilot_compute_description["vm_ssh_keyfile"]

        session = saga.Session()
        session.add_context(ctx)
                
        TRIAL_MAX=30
        trials=0
        while trials < TRIAL_MAX:
            try:
                js = saga.job.Service(url, session=session)
                logger.debug("Job Description Type: " + str(type(self.job_description)))
                job = js.create_job(self.job_description)
                logger.debug("Attempt: %d, submit pilot job to: %s "%(trials,str(url)))
                job.run()
                if job.get_state()==saga.job.FAILED:
                    logger.warning("Submission failed.")
                    trials = trials + 1 
                    time.sleep(30)
                    continue
                else:
                    break
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                logger.warning("Submission failed: " + str(exc_value))
                #self.__print_traceback()
                trials = trials + 1 
                time.sleep(30)
                if trials == TRIAL_MAX:
                    raise Exception("Submission of agent failed.") 
                
        logger.debug("Job State : %s" % (job.get_state())) 
        
        

    def wait_for_running(self):
        while self.get_state()!=State.RUNNING:
            time.sleep(5)
        
    
    def get_state(self):
        result = State.UNKNOWN
        try:
            self.instance.update()
            result=self.instance.state
        except:
            logger.warning("Instance not reachable/active yet...")
        return result
    
    
    def cancel(self):
        self.instance.terminate()
        
        
    ###########################################################################
    # private methods
    def __print_traceback(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "*** print_tb:"
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        print "*** print_exception:"
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)
    
 
     
if __name__ == "__main__":
    ec2_service = Service("ec2+ssh://aws.amazon.com")
    j = ec2_service.create_job("blas")
    j.run()
    print j.get_state()
