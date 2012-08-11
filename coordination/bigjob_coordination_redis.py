'''
Encapsulates coordination and communication specifics of bigjob
'''
import logging
import threading
import datetime
import sys
import os
import pickle
import pdb
import time

from bigjob import logger
import redis

if sys.version_info < (2, 5):
    sys.path.append(os.path.dirname( os.path.abspath( __file__) ) + "/../ext/uuid-1.30/")
    sys.stderr.write("Warning: Using unsupported Python version\n")
    
logging.debug(str(sys.path))
import uuid



REDIS_SERVER="localhost"
REDIS_SERVER_PORT=6379
REDIS_URL_SCHEME="redis://"

class bigjob_coordination(object):
    '''
    Encapsulates communication and coordination
    Implementation based on Redis (http://redis.io)
    '''

    def __init__(self, server=REDIS_SERVER, server_port=REDIS_SERVER_PORT, server_connect_url=None,
                 username=None, password=None, dbtype=None, url_prefix=None):
        '''
        Constructor
        '''
        if server_port==None:
            server_port=6379
            
        self.username = None
        self.password = None   
        
        self.address = "%s%s:%i"%(REDIS_URL_SCHEME, server, server_port)
        self.dbtype=""
        if server_connect_url!=None:
            self.address=server_connect_url    
            start_index = self.address.find(REDIS_URL_SCHEME)+len(REDIS_URL_SCHEME)
            server_and_port = self.address[start_index:]
            password_end = server_and_port.find("@") 
            # parse out password
            if password_end != -1:
                self.password = server_and_port[:password_end]
                start_index=password_end
                server_and_port= server_and_port[(password_end+1):]
            
            # port and hostname
            if server_and_port.find(":")==-1:
                server=server_and_port
                server_port = REDIS_SERVER_PORT
            else:
                server = server_and_port.split(":")[0]
                server_port = int(server_and_port.split(":")[1])
        else:
            self.password = username
            if self.password != None and self.password!="":
                self.address = "%s%s@%s:%i"%(REDIS_URL_SCHEME, self.password, server, server_port)
        
        logger.debug("Connect to Redis: " + server + " Port: " + str(server_port))
        
        if self.password==None:
            self.redis_client = redis.Redis(host=server, port=server_port, db=0)
        else:
            self.redis_client = redis.Redis(host=server, port=server_port, password=self.password, db=0)
        #self.redis_client_pubsub = self.redis_client.pubsub() # redis pubsub client       
        #self.resource_lock = threading.RLock()
        self.pipe = self.redis_client.pipeline()
        try:
            self.redis_client.ping()
        except:
            logger.error("Please start Redis server!")
            raise Exception("Please start Redis server!")
        
        
    def get_address(self):
        return self.address
    
    #####################################################################################
    # Pilot-Job State
    def set_pilot_state(self, pilot_url, new_state, stopped=False):     
        logger.debug("update state of pilot job to: " + str(new_state) 
                     + " stopped: " + str(stopped))
        self.redis_client.hmset(pilot_url, {"state":str(new_state), "stopped":str(stopped)})
        if stopped==True:
            self.queue_job(pilot_url, "STOP")
        
        
    def get_pilot_state(self, pilot_url):
        state = self.redis_client.hgetall(pilot_url)
        return state
    
    
    #####################################################################################
    # Pilot-Job State
    def set_pilot_description(self, pilot_url, description):     
        logger.debug("update description of pilot job to: " + str(description))
        self.redis_client.hmset(pilot_url + ":description", {"description":description})
        
    def get_pilot_description(self, pilot_url):
        description = self.redis_client.hgetall(pilot_url + ":description")
        return description
    
    #def is_pilot_stopped(self,pilot_url):
    #    state = self.redis_client.hgetall(pilot_url)
    #    if state==None or not state.has_key("stopped"):
    #        return True        
    #    return state["stopped"]
    
    def get_jobs_of_pilot(self, pilot_url):
        """ returns array of job_url that are associated with a pilot """
        jobs = self.redis_client.keys(pilot_url+":jobs:*")
        jobs_fqdn = [os.path.join(self.get_address(), i)for i in jobs] 
        return jobs_fqdn
    
    
    def delete_pilot(self, pilot_url):
        items = self.redis_client.keys(pilot_url+"*")  
        for i in items:        
            self.pipe.delete(i)
        self.pipe.execute()
    
    #####################################################################################
    # Sub-Job State    
    def set_job_state(self, job_url, new_state):
        #self.resource_lock.acquire()        
        logger.debug("set job state to: " + str(new_state))
        self.redis_client.hset(job_url, "state", str(new_state))
        
        if new_state=="Unknown":
            self.redis_client.hset(job_url,"start_time", str(time.time()))
        elif new_state=="Running":
            self.redis_client.hset(job_url,"end_queue_time", str(time.time()))
        elif new_state=="Done":
            self.redis_client.hset(job_url, "end_time", str(time.time()))
       
        
        #self.resource_lock.release()
        
    def get_job_state(self, job_url):
        return self.redis_client.hget(job_url, "state")      
    
    
    #####################################################################################
    # Sub-Job Description
    def set_job(self, job_url, job_dict):
        self.redis_client.hmset(job_url, job_dict)
        self.set_job_state(job_url, "Unknown")
        
    def get_job(self, job_url):
        return self.redis_client.hgetall(job_url)    
    
    def delete_job(self, job_url):
        self.redis_client.delete(job_url+"*")
    
    
    #####################################################################################
    # Distributed queue for sub-jobs
    def queue_job(self, pilot_url, job_url):
        """ queue new job to pilot """
        queue_name = pilot_url + ":queue"
        self.redis_client.set(queue_name + ':last_in', pickle.dumps(datetime.datetime.now()))
        self.redis_client.lpush(queue_name, job_url)
                
        
    def dequeue_job(self, pilot_url):
        """ deque to new job  of a certain pilot """
        queue_name = pilot_url + ":queue"        
        logger.debug("Dequeue sub-job from: " + queue_name 
                      + " number queued items: " + str(self.redis_client.llen(queue_name)))
        self.redis_client.set(queue_name + ':last_out', pickle.dumps(datetime.datetime.now()))
        job_url = self.redis_client.brpop(queue_name, 1)
        if job_url==None:
            return job_url
        logger.debug("Dequeued: " + str(job_url))
        return job_url[1]
    
