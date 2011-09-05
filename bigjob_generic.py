#!/usr/bin/env python

"""Module big_job.

This Module is used to launch jobs via the advert service. 

It assumes that an bigjob_agent.py is available on the remote machine.
bigjob_agent.py will poll the advert service for new jobs and run these jobs on the respective
machine .

Background: This approach avoids queueing delays since the igjob_agent_launcher.py must be just started via saga.job or saga.cpr
once. All shortrunning task will be started using the protocol implemented by subjob() and bigjob_agent.py

Installation:
Set environment variable BIGJOB_HOME to installation directory

"""

import sys
import saga
import time
import pdb
import os
import traceback
import logging
logging.basicConfig(level=logging.DEBUG)

# import other BigJob packages
# import API
import api.base


if sys.version_info < (2, 5):
    sys.path.append(os.path.dirname( __file__ ) + "/ext/uuid-1.30/")
    sys.stderr.write("Warning: Using unsupported Python version\n")
if sys.version_info < (2, 4):
    sys.path.append(os.path.dirname( __file__ ) + "/ext/subprocess-2.6.4/")
    sys.stderr.write("Warning: Using unsupported Python version\n")
if sys.version_info < (2, 3):
    sys.stderr.write("Error: Python versions <2.3 not supported\n")
    sys.exit(-1)

import uuid

def get_uuid():
    wd_uuid=""
    wd_uuid += str(uuid.uuid1())
    return wd_uuid


""" Config parameters (will move to config file in future) """
APPLICATION_NAME="bigjob"
CLEANUP=True

# Support for multiple coordination backends (ZMQ and Redis)
BACKEND = "REDIS" #{REDIS, ZMQ}

if BACKEND=="ZMQ":
    try:
        from bigjob_coordination_zmq import bigjob_coordination
        logging.debug("Utilizing ZMQ Backend")
    except:
        logging.error("ZMQ Backend not found. Please install ZeroMQ (http://www.zeromq.org/intro:get-the-software) and " 
                      +"PYZMQ (http://zeromq.github.com/pyzmq/)")
else:
    try:
        from bigjob_coordination_redis import bigjob_coordination        
        logging.debug("Utilizing Redis Backend. Please make sure Redis server is configured in bigjob_coordination_redis.py.")
    except:
        logging.error("Error loading PyRedis.")


#for legacy purposes and support for old BJ API
pilot_url_dict={} # stores a mapping of pilot_url to bigjob

class BigJobError(Exception):
    def __init__(self, value):
        self.value = value
    
    def __str__(self):
        return repr(self.value)
    

class bigjob(api.base.bigjob):
    
    def __init__(self, database_host):        
        self.database_host = database_host
        print "init advert service session at host: " + database_host
        self.uuid = get_uuid()
        
        self.coordination = bigjob_coordination()
        
        self.app_url = APPLICATION_NAME +":" + str(self.uuid) 
        
        #self.app_url = saga.url(ADVERT_URL_SCHEME+ database_host + "/"+APPLICATION_NAME + "-" + str(self.uuid) + "/")
        #self.app_dir = saga.advert.directory(self.app_url, saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        self.state=saga.job.Unknown
        self.pilot_url=""
        self.job = None
        print "initialized BigJob: " + self.app_url
    
    def start_pilot_job(self, 
                 lrms_url, 
                 bigjob_agent_executable,
                 number_nodes,
                 queue,
                 project,
                 working_directory,
                 userproxy,
                 walltime,
                 processes_per_node=1):
        
        
        if self.job != None:
            raise BigJobError("One BigJob already active. Please stop BigJob first.") 
            return

        ##############################################################################
        # initialization of coordination and communication subsystem
        # Redis initialization
        lrms_saga_url = saga.url(lrms_url)
        self.pilot_url = self.app_url + ":" + lrms_saga_url.host
        pilot_url_dict[self.pilot_url]=self
        
        logging.debug("create pilot job entry on backend server: " + self.pilot_url)
        self.coordination.set_pilot_state(self.pilot_url, str(saga.job.Unknown), False)
        
        #self.redis.hmset(self.pilot_url, {"state":str(saga.job.Unknown), "stopped":"false"}) 
        
        logging.debug("set pilot state to: " + str(saga.job.Unknown))
        ##############################################################################
                
        
        self.number_nodes=int(number_nodes)        
        
        # discover location of agent executable
        if bigjob_agent_executable==None:            
            if os.getenv("BIGJOB_HOME", None)!=None:
                bigjob_agent_executable=os.getenv("BIGJOB_HOME")+"/bigjob_agent_launcher.sh"
            else:
                bigjob_agent_executable=os.getcwd()+"/bigjob_agent_launcher.sh"
 
        # create job description
        jd = saga.job.description()
        jd.number_of_processes = str(number_nodes)
        jd.processes_per_host=str(processes_per_node)
        jd.spmd_variation = "single"
        jd.arguments = [bigjob_agent_executable, self.coordination.get_address(), self.pilot_url]
        jd.executable = "/bin/bash"
        #jd.executable = bigjob_agent_executable
        if queue != None:
            jd.queue = queue
        if project !=None:
            jd.job_project = [project]
        if walltime!=None:
            jd.wall_time_limit=str(walltime)

        # XXX Isn't the working directory about the remote site?
        if working_directory != None:
            if not os.path.isdir(working_directory) and lrms_saga_url.scheme=="fork":
                os.mkdir(working_directory)
            jd.working_directory = working_directory
        else:
            jd.working_directory = "$(HOME)"
            
        print "Working directory: " + jd.working_directory
        
        jd.output = "stdout-bigjob_agent-" + str(self.uuid) + ".txt"
        jd.error = "stderr-bigjob_agent-" + str(self.uuid) + ".txt"
           
        # Submit job
        js = None	
        if userproxy != None and userproxy != '':
      	    s = saga.session()
            os.environ["X509_USER_PROXY"]=userproxy
            ctx = saga.context("x509")
            ctx.set_attribute ("UserProxy", userproxy)
            s.add_context(ctx)
            print "use proxy: " + userproxy
            js = saga.job.service(s, lrms_saga_url)
        else:
            print "use standard proxy"
            js = saga.job.service(lrms_saga_url)

        self.job = js.create_job(jd)
        print "Submit pilot job to: " + str(lrms_saga_url)
        self.job.run()
        #return self.job
     
    def add_subjob(self, jd, job_url, job_id):
        logging.debug("add subjob to queue of PJ: " + str(self.pilot_url))        
        for i in range(0,3):
            try:
                logging.debug("initialized redis dictionary for job: " + job_url)
                # put job description attributes to Redis
                job_dict = {}
                attributes = jd.list_attributes()                
                for i in attributes:          
                        if jd.attribute_is_vector(i):
                            logging.debug("Add attribute: " + str(i) + " Value: " + str(jd.get_vector_attribute(i)))
                            vector_attr = []
                            for j in jd.get_vector_attribute(i):
                                vector_attr.append(j)
                            job_dict[i]=vector_attr
                        else:
                            logging.debug("Add attribute: " + str(i) + " Value: " + jd.get_attribute(i))
                            job_dict[i] = jd.get_attribute(i)
                
                job_dict["state"] = str(saga.job.Unknown)
                job_dict["job-id"] = str(job_id)
                
                logging.debug("update job description at communication & coordination sub-system")
                self.coordination.set_job(job_url, job_dict)                                                
                self.coordination.queue_job(self.pilot_url, job_url)
                break
            except:
                traceback.print_exc(file=sys.stdout)
                time.sleep(2)
                #raise Exception("Unable to submit job")
                     
    def delete_subjob(self, job_url):
        self.coordination.delete_job(job_url) 
    
    def get_subjob_state(self, job_url):
        return self.coordination.get_job_state(job_url) 
     
    def get_state(self):        
        """ duck typing for get_state of saga.cpr.job and saga.job.job  """
        try:
            return self.job.get_state()
        except:
            return None
    
    def get_state_detail(self): 
        try:
            return self.coordination.get_pilot_state(self.pilot_url)
        except:
            return None
    
    def get_free_nodes(self):
        jobs = self.coordination.get_jobs_of_pilot(self.pilot_url)
        number_used_nodes=0
        for i in jobs:
            job_detail = self.coordination.get_job(i)            
            if job_detail != None and job_detail.has_key("state") == True\
                and job_detail["state"]==str(saga.job.Running):
                job_np = "1"
                if (job_detail["NumberOfProcesses"] == True):
                    job_np = job_detail["NumberOfProcesses"]
                number_used_nodes=number_used_nodes + int(job_np)
        return (self.number_nodes - number_used_nodes)

    
    def stop_pilot_job(self):
        """ mark in advert directory of pilot-job as stopped """
        try:
            print "stop pilot job: " + self.pilot_url
            self.coordination.set_pilot_state(str(saga.job.Done), True)
            self.cancel()
            self.job=None
        except:
            pass
    
    def cancel(self):        
        """ duck typing for cancel of saga.cpr.job and saga.job.job  """
        print "Cancel Pilot Job"
        try:
            self.job.cancel()
        except:
            pass
            #traceback.print_stack()
        try:            
            print "delete pilot job: " + str(self.pilot_url)                      
            if CLEANUP:
                self.coordination.delete_pilot(self.pilot_url)                
        except:
            pass
            #traceback.print_stack()

    def __repr__(self):
        return self.pilot_url 

    def __del__(self):
        self.cancel()

                    
                    
class subjob(api.base.subjob):
    
    def __init__(self, database_host):
        """Constructor"""
        self.database_host = database_host
        self.job_url=None
        self.uuid = get_uuid()
        self.job_url = None
        self.pilot_url = None
        self.bj = None
        
    def get_job_url(self, pilot_url):
        self.job_url = pilot_url + ":jobs:" + str(self.uuid)
        return self.job_url
    

    def submit_job(self, pilot_url, jd):
        """ submit subjob to referenced bigjob """
        if self.job_url==None:
            self.job_url=self.get_job_url(pilot_url)            
        
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=pilot_url_dict[pilot_url]    
        self.bj.add_subjob(jd, self.job_url, self.uuid)


    def get_state(self, pilot_url=None):        
        """ duck typing for saga.job  """
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=pilot_url_dict[pilot_url]                
        return self.bj.get_subjob_state(self.job_url)
    
    
    def cancel(self, pilot_url=None):
        print "delete job: " + self.job_url
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=pilot_url_dict[pilot_url]  
        self.bj.delete_subjob(self.job_url)        

    def __del__(self):
        self.cancel()
    
    def __repr__(self):        
        if(self.job_url==None):
            return "None"
        else:
            return self.job_url

