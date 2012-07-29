#!/usr/bin/env python

from bigjob import logger
import os
import uuid
import time

from boto.ec2.connection import EC2Connection

import bliss.saga as saga

###############################################################################
# EC2 General
EC2_SSH_PRIVATE_KEY_FILE="/Users/luckow/.ec2/id-lsu-keypair"
EC2_INSTANCE_TYPE="t1.micro"
EC2_PLACEMENT_GROUP=None
EC2_SECURITY_GROUP="default"

# VM/Image specific configurations
EC2_AMI_ID="ami-c7943cae" #  ami-82fa58eb official Amazon Ubuntu 12.04 LTS (requires dev tools installation)
EC2_USERNAME="ubuntu"
EC2_KEYNAME="lsu-keypair"

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
    PENDING="pending"
    RUNNING="running"
    
    
class Service(object):
    """ Plugin for Amazon EC2 
    
        Manages endpoint in the form of:
        
            ec2+ssh://<EC2 Endpoint>    
    """

    def __init__(self, resource_url):
        """Constructor"""
        self.resource_url = resource_url
            
    
    def create_job(self, job_description):
        j = Job(job_description, self.resource_url)
        return j
            
    
    def __del__(self):
        pass
    
    
    

class Job(object):
    """ Plugin for Amazon EC2 
    
        Starts VM and executes BJ agent on this VM    
    """

    def __init__(self, job_description, saga_url):
        
        self.job_description = job_description
        self.saga_url = saga_url
        
        self.id="bigjob-" + str(uuid.uuid1())
        self.network_ip=None
        
        self.ec2_conn = EC2Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        self.instance = None
        
        
    def run(self):
        reservation = self.ec2_conn.run_instances(EC2_AMI_ID,
                                    key_name=EC2_KEYNAME,
                                    instance_type=EC2_INSTANCE_TYPE,
                                    security_groups=[EC2_SECURITY_GROUP])
        self.instance = reservation.instances[0]
        self.instance_id = self.instance.id
        logger.debug("Started EC2 instance: %s"%self.instance_id)
        self.ec2_conn.create_tags([self.instance_id], {"Name": self.id})
        self.wait_for_running()
        
        self.network_ip = self.instance.ip_address 
        url = "ssh://" + str(self.network_ip)
        logger.debug("Connect to: %s"%(url))
        js = saga.job.Service(url)
        
        # Submit job
        ctx = saga.Context()
        ctx.type = saga.Context.SSH
        ctx.userid = EC2_USERNAME
        ctx.userkey = EC2_SSH_PRIVATE_KEY_FILE
        js.session.contexts.append(ctx)

        job = js.create_job(self.job_description)
        trials=0
        while trials < 3:
            try:
                print ("Attempt: %d, submit pilot job to: %s "%(trials,str(url)))
                job.run()
                break
            except:
                trials = trials + 1 
                time.sleep(7)
                
        print "Job State : %s" % (job.get_state()) 
        
        

    def wait_for_running(self):
        while self.get_state()!=State.RUNNING:
            time.sleep(5)
        
    
    def get_state(self):
        self.instance.update()
        result=self.instance.state
        return result
    
    
    def cancel(self):
        self.instance.terminate()
        
        
    ###########################################################################
    # private methods
    
 
     
if __name__ == "__main__":
    ec2_service = Service("ec2+ssh://aws.amazon.com")
    j = ec2_service.create_job("blas")
    j.run()
    print j.get_state()
