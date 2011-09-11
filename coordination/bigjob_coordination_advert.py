'''
Encapsulates coordination and communication specifics of bigjob
'''


import logging
import threading
import datetime
import time
import sys
import os
import pickle
import pdb
import saga
import json
import urlparse

if sys.version_info < (2, 5):
    sys.path.append(os.path.dirname( os.path.abspath( __file__) ) + "/../ext/uuid-1.30/")
    sys.stderr.write("Warning: Using unsupported Python version\n")
    
logging.debug(str(sys.path))
import uuid

APPLICATION_NAME="BigJob/BigJob"
ADVERT_URL_SCHEME = "advert://"
ADVERT_SERVER="advert.cct.lsu.edu"
ADVERT_SERVER_PORT=8080

class bigjob_coordination(object):
    '''
    Encapsulates communication and coordination
    Implementation based on Redis (http://redis.io)
    '''

    def __init__(self, server=ADVERT_SERVER, server_port=ADVERT_SERVER_PORT, server_connect_url=""):
        '''
        Constructor
        '''
        if server_port != None:
            self.address = ADVERT_URL_SCHEME+"%s:%i"%(server, server_port)
        else:
            self.address = ADVERT_URL_SCHEME+"%s"%(server)
            
        self.pilot_url = self.address
        
    def get_address(self):
        return self.address
    
    def get_url(self, id_string):
        try:
            path = urlparse.urlparse(id_string).path  
        except:
            pass        
        
        path = path.replace(":", "/")
        url_string = self.address + "/" + path
        return url_string
        
        
    #####################################################################################
    # Pilot-Job State
    def set_pilot_state(self, pilot_url, new_state, stopped=False):   
        pilot_url = self.get_url(pilot_url)
        logging.debug("create advert entry: " + pilot_url)
        pilot_dir = saga.advert.directory(saga.url(pilot_url), saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        logging.debug("update state of pilot job to: " + str(new_state))
        pilot_dir.set_attribute("state", str(new_state)) 
        pilot_dir.set_attribute("stopped", str(stopped))
        
    def get_pilot_state(self, pilot_url):
        pilot_url = self.get_url(pilot_url)
        pilot_dir = saga.advert.directory(saga.url(pilot_url), saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        state = pilot_dir.get_attribute("state")
        stopped = pilot_dir.get_attribute("stopped") 
        if stopped == "false":
            return {"state":state, "stopped":False}
        else:
            return {"state":state, "stopped":True}        
        
    
    def get_jobs_of_pilot(self, pilot_url):
        pilot_url = self.get_url(pilot_url)
        """ returns array of job_url that are associated with a pilot """
        pilot_dir = saga.advert.directory(saga.url(pilot_url), saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        jobs = pilot_dir.list()        
        return jobs
    
    def delete_pilot(self, pilot_url):
        pilot_url = self.get_url(pilot_url)
        pilot_dir = saga.advert.directory(saga.url(pilot_url), saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        pilot_url.remove(pilot_url, saga.name_space.Recursive)    
    
    #####################################################################################
    # Sub-Job State    
    def set_job_state(self, job_url, new_state):        
        job_url = self.get_url(job_url)
        logging.debug("Set state of job: " + str(job_url))
        job_dir = saga.advert.directory(saga.url(job_url), saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        job_dir.set_attribute("state", str(new_state))
        
        
    def get_job_state(self, job_url):        
        job_url = self.get_url(job_url)        
        job_dir = saga.advert.directory(saga.url(job_url), saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        state = job_dir.get_attribute("state")  
        #logging.debug("Get state of job: " + str(job_url) + " state: " + str(state))
        return state      
    
    
    #####################################################################################
    # Sub-Job Description
    def set_job(self, job_url, job_dict):
        job_url = self.get_url(job_url)
        job_dir = saga.advert.directory(saga.url(job_url), 
                                        saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        job_desc_entry = saga.advert.entry(saga.url(job_url+"/job-description"),
                                           saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        logging.debug("initialized advert entry for job: " + job_url)
        job_desc_entry.store_string(json.dumps(job_dict))
        self.set_job_state(job_url, str(saga.job.Unknown))
        
        
    
    def get_job(self, job_url):
        #job_dir = saga.advert.directory(saga.url(job_url), 
        #                                saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        job_url = self.get_url(job_url)
        job_desc_entry = saga.advert.entry(saga.url(job_url+"/job-description"),
                                           saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        job_dict = json.loads(job_desc_entry.retrieve_string())
        return job_dict    
    
    def delete_job(self, job_url):
        job_url = self.get_url(job_url)
        job_dir = saga.advert.directory(saga.url(job_url), 
                                        saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        job_dir.remove(job_url, saga.name_space.Recursive)  
    
    
    #####################################################################################
    # Distributed queue for sub-jobs
    def queue_job(self, pilot_url, job_url):
        pilot_url = self.get_url(pilot_url)
        job_url = self.get_url(job_url)
        """ queue new job to pilot """
        new_job_url = pilot_url + "/new/" + str(uuid.uuid1())
        new_job_dir = saga.advert.directory(saga.url(new_job_url), 
                                            saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        new_job_dir.set_attribute("joburl", job_url)
                
        
    def dequeue_job(self, pilot_url):
        """ deque to new job  of a certain pilot """
        pilot_url = self.get_url(pilot_url)
        jobs = []        
        new_job_dir_url = pilot_url + "/new/" 
        new_job_dir = saga.advert.directory(saga.url(new_job_dir_url), 
                                            saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        new_jobs = new_job_dir.list()
        logging.debug("Base dir: " + new_job_dir_url + " Number New jobs: " + str(len(new_jobs)));
        if len(new_jobs)>=1:
            job_entry=new_jobs[0]     
            job_dir_url = new_job_dir_url + "/" + job_entry.get_string()       
            logging.debug("open job at " + str(job_dir_url))
            job_dir = saga.advert.directory(saga.url(job_dir_url), 
                                       saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
            
            #new_job_dir.open_dir(job_entry)                        
            job_url = job_dir.get_attribute("joburl")
            #remove old job entry
            job_dir.remove(job_dir_url, saga.name_space.Recursive)    
            logging.debug("new job: " + str(job_url))
            return job_url
        else:
            time.sleep(1)
            return 
            
            
            
            
