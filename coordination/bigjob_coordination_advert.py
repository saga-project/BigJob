'''
Encapsulates coordination and communication specifics of bigjob
'''
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
import logging

from bigjob import logger
logger.debug("Load Advert Coordination")

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

    def __init__(self, server=ADVERT_SERVER, server_port=ADVERT_SERVER_PORT, 
                       server_connect_url=None, username=None, password=None, 
                       dbtype=None, url_prefix=None):
        '''
        Constructor
        '''
        
        #pdb.set_trace()
        if url_prefix==None:
            url_prefix = ADVERT_URL_SCHEME
             
        if username!=None and username!="":
            url_prefix   = url_prefix+username
            if password!=None:
                url_prefix = url_prefix + ":" + password
            url_prefix = url_prefix + "@"
        if server_connect_url!=None:
            self.address=server_connect_url
        elif server_port != None:
            self.address = url_prefix+"%s:%i"%(server, server_port)
        elif server != None:
            self.address = url_prefix+"%s"%(server)           
        
        self.username=""
        self.password=""
        self.dbtype=""
        surl = saga.Url(self.address)
        if server_connect_url==None: # Manager
            if username!=None:
                surl.username=username    
                self.username=username        
            if password != None:
                surl.password = password
                self.password=password
            if dbtype != None:            
                #surl.query = dbtype
                self.dbtype = dbtype
        else: # Agent
            if surl.query!=None:
                self.dbtype=surl.query
                surl.query=""
                
        self.address = str(surl)
        self.pilot_url = self.address
        logger.debug("Server: " + str(server) + " Port " + str(server_port) +
                      " Url prefix: " + str(url_prefix) + 
                      " Address: " + str(self.get_address()) +
                      " server_connect_url: " + str(server_connect_url) )
        logger.debug("Initialized Coordination to: %s (DB: %s)"%(self.address, self.dbtype))
        self.resource_lock = threading.RLock()
        
    
    def get_address(self):
        return self.address + "?" + self.dbtype
    
    
    def get_url(self, id_string):        
        
        if not id_string.startswith("advert") and not id_string.startswith("sqlasyncadvert"): 
            path = id_string.replace(":", "/")            
            if self.dbtype!=None:
                if self.dbtype.endswith("?"):
                    self.dbtype = self.dbtype[:-1]
                url_string = self.address + "/" + path + "?" + self.dbtype
            else:
                url_string = self.address + "/" + path 
            return url_string
        
        
        if self.dbtype!=None:
            if self.dbtype.endswith("?"):
                self.dbtype = self.dbtype[:-1]
            id_string = id_string + "?" + self.dbtype
            
             
        return id_string
 
        
    #####################################################################################
    # Pilot-Job State
    def set_pilot_state(self, pilot_url, new_state, stopped=False):   
        pilot_url = self.get_url(pilot_url)
        logger.debug("create advert entry: " + pilot_url)
        pilot_dir = saga.advert.directory(saga.url(pilot_url), saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        logger.debug("update state of pilot job to: " + str(new_state) + " Stopped: " + str(stopped))
        pilot_dir.set_attribute("state", str(new_state)) 
        pilot_dir.set_attribute("stopped", str(stopped))
        
    def get_pilot_state(self, pilot_url):
        pilot_url = self.get_url(pilot_url)
        pilot_dir = saga.advert.directory(saga.url(pilot_url), saga.advert.Read)
        state = pilot_dir.get_attribute("state")
        stopped = pilot_dir.get_attribute("stopped") 
        if stopped == "false" or stopped == "False":
            return {"state":state, "stopped":False}
        else:
            return {"state":state, "stopped":True}        
        
    #####################################################################################
    # Pilot-Job Description
    def set_pilot_description(self, pilot_url, description):     
        pass
    
    def get_pilot_description(self, pilot_url):
        pass
    
    def get_jobs_of_pilot(self, pilot_url):
        pilot_url = self.get_url(pilot_url + "/jobs")
        """ returns array of job_url that are associated with a pilot """
        pilot_dir = saga.advert.directory(saga.url(pilot_url), saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        jobs = pilot_dir.list()   
        job_urls = [self.__get_colon_url(self.__remove_dbtype(pilot_url) + "/" + i.get_string()) for i in jobs]
        if self.dbtype!=None:
            job_urls = [i + "?" + self.dbtype for i in job_urls]
        return job_urls
    
    
    def delete_pilot(self, pilot_url):
        pilot_url = self.get_url(pilot_url)
        pilot_dir = saga.advert.directory(saga.url(pilot_url), saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        pilot_dir.remove(pilot_url, saga.name_space.Recursive)    
    
    
    #####################################################################################
    # Sub-Job State    
    def set_job_state(self, job_url, new_state):   
        self.resource_lock.acquire()     
        job_url = self.get_url(job_url)
        logger.debug("Set state of job: " + str(job_url) + " to: " + str(new_state))
        job_dir = saga.advert.directory(saga.url(job_url), saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        job_dir.set_attribute("state", str(new_state))
        if new_state=="Unknown":
            job_dir.set_attribute("start_time", str(time.time()))
        if new_state=="Running":
            job_dir.set_attribute("end_queue_time", str(time.time()))
        elif new_state=="Done":
            job_dir.set_attribute("end_time", str(time.time()))
        self.resource_lock.release()
        
    def get_job_state(self, job_url):   
        job_url = self.get_url(self.__remove_dbtype(job_url))        
        job_dir = saga.advert.directory(saga.url(job_url), saga.advert.Read)
        state = job_dir.get_attribute("state")  
        #logger.debug("Get state of job: " + str(job_url) + " state: " + str(state))
        return state      
    
    
    #####################################################################################
    # Sub-Job Description
    def set_job(self, job_url, job_dict):
        job_dir_url = self.get_url(job_url)
        job_description_url = self.get_url(job_url+"/job-description")
        logger.debug("Job URL: %s, Job Description URL: %s"%(job_dir_url, job_description_url))
        #job_dir = saga.advert.directory(saga.url(job_dir_url), 
        #                                saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        # directory is recursively created
        job_desc_entry = saga.advert.entry(saga.url(job_description_url),
                                           saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        logger.debug("initialized advert entry for job: " + job_dir_url)
        job_desc_entry.store_string(json.dumps(job_dict))
        self.set_job_state(job_url, str(saga.job.Unknown))
        
        
    
    def get_job(self, job_url):
        #job_dir = saga.advert.directory(saga.url(job_url), 
        #                                saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        job_url = self.get_url(job_url+"/job-description")
        logger.debug("Get job description from: %s"%(job_url))
        job_desc_entry = saga.advert.entry(saga.url(job_url),
                                           saga.advert.Read)
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
        self.resource_lock.acquire()
        #pilot_url = self.get_url(pilot_url)
        job_url = self.get_url(job_url)
        """ queue new job to pilot """
        new_job_url = self.get_url(pilot_url + "/new/" + str(uuid.uuid1()))
        logger.debug("Job URL: %s Create new job entry at: %s"%(job_url,new_job_url))
        new_job_dir = saga.advert.directory(saga.url(new_job_url), 
                                            saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        new_job_dir.set_attribute("joburl", job_url)
        self.resource_lock.release()
        
    def dequeue_job(self, pilot_url):
        """ deque to new job  of a certain pilot """
        self.resource_lock.acquire()
        #pilot_url = self.get_url(pilot_url)
        jobs = []        
        new_job_dir_url = self.get_url(pilot_url + "/new/") 
        new_job_dir = saga.advert.directory(saga.url(new_job_dir_url), 
                                            saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        new_jobs = new_job_dir.list()
        logger.debug("Pilot Job base dir: " + new_job_dir_url + " #new jobs: " + str(len(new_jobs))
                      + " jobs: " + str(new_jobs));
        if len(new_jobs)>=1:
            job_entry=new_jobs[0]     
            job_dir_url = self.get_url(pilot_url + "/new/" + "/" + job_entry.get_string())       
            logger.debug("Open job at " + str(job_dir_url))
            job_dir = saga.advert.directory(saga.url(job_dir_url), 
                                       saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
            
            #new_job_dir.open_dir(job_entry)                        
            job_url = job_dir.get_attribute("joburl")
            #remove old job entry
            job_dir.remove(self.__remove_dbtype(job_dir_url), saga.name_space.Recursive)
                
            logger.debug("Dequeued new job: " + str(job_url))
            self.resource_lock.release()
            return self.__remove_dbtype(job_url)
        else:
            self.resource_lock.release()
            time.sleep(1)
            return 
            
    ###########################################################################
    # Private internal methods
            
    def __remove_dbtype(self, url):
        surl = saga.url(url)
        surl.query=""
        return str(surl)  
            
         
    def __get_colon_url(self, id_string):
        surl = saga.url(id_string)
        path = surl.path[1:]
        new_path = path.replace("/", ":")
        surl.path = "/" + new_path
        return surl.get_string()     
