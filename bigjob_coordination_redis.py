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


sys.path.insert(0, (os.path.dirname(os.path.abspath( __file__) ) + "/../ext/redis-2.4.9/"))
from redis import *

if sys.version_info < (2, 5):
    sys.path.append(os.path.dirname( os.path.abspath( __file__) ) + "/../ext/uuid-1.30/")
    sys.stderr.write("Warning: Using unsupported Python version\n")
    
logging.debug(str(sys.path))
import uuid

REDIS_SERVER="localhost"
REDIS_SERVER_PORT=6379


class bigjob_coordination_redis(object):
    '''
    Encapsulates communication and coordination
    Implementation based on Redis (http://redis.io)
    '''

    def __init__(self, server=REDIS_SERVER, server_port=REDIS_SERVER_PORT, server_connect_url=""):
        '''
        Constructor
        '''
        self.address = "redis://%s:%i"%(server, server_port)
        self.redis = Redis(host=server, port=server_port, db=0)
        self.redis_pubsub = self.redis.pubsub() # redis pubsub client       
        self.resource_lock = threading.RLock()
        self.pipe = self.redis.pipeline()
        try:
            self.redis.ping()
        except Exception as e:
            logging.error("Please start Redis server!")
            raise e
        
    def get_address(self):
        return ("redis://")
    #####################################################################################
    # Pilot-Job State
    def set_pilot_state(self, pilot_url, new_state, stopped=False):     
        logging.debug("update state of pilot job to: " + str(new_state))
        self.redis.hmset(pilot_url, {"state":str(new_state), "stopped":str(stopped)})
        
    def get_pilot_state(self, pilot_url):
        state = self.redis.hgetall(pilot_url)
        return state
    
    def get_jobs_of_pilot(self, pilot_url):
        """ returns array of job_url that are associated with a pilot """
        return self.redis.keys(pilot_url+":jobs:*") 
    
    def delete_pilot(self, pilot_url):
        items = self.redis.keys(pilot_url+"*")  
        for i in items:        
            self.pipe.delete(i)
        self.pipe.execute()
    
    #####################################################################################
    # Sub-Job State    
    def set_job_state(self, job_url, new_state):
        #self.resource_lock.acquire()        
        self.redis.hset(job_url, "state", str(new_state))
        #self.resource_lock.release()
        
    def get_job_state(self, job_url):
        return self.redis.hget(job_url, "state")      
    
    
    #####################################################################################
    # Sub-Job Description
    def set_job(self, job_url, job_dict):
        self.redis.hmset(job_url, job_dict)
    
    def get_job(self, job_url):
        return self.redis.hgetall(job_url)    
    
    def delete_job(self, job_url):
        self.redis.delete(job_url+"*")
    
    
    #####################################################################################
    # Distributed queue for sub-jobs
    def queue_job(self, pilot_url, job_url):
        """ queue new job to pilot """
        queue_name = pilot_url + ":queue"
        self.redis.set(queue_name + ':last_in', pickle.dumps(datetime.datetime.now()))
        self.redis.lpush(queue_name, job_url)
                
        
    def dequeue_job(self, pilot_url):
        """ deque to new job  of a certain pilot """
        queue_name = pilot_url + ":queue"        
        logging.debug("Dequeue sub-job from: " + queue_name 
                      + " number queued items: " + str(self.redis.llen(queue_name)))
        self.redis.set(queue_name + ':last_out', pickle.dumps(datetime.datetime.now()))
        job_url = self.redis.brpop(queue_name, 120)
        if job_url==None:
            return job_url
        logging.debug("Dequeued: " + str(job_url))
        return job_url[1]
    
