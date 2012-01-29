import sys
import getopt
import saga
import time
import pdb
import os
import traceback
import logging
import uuid

import bigjob

from bigjob import logger
from bigjob import bigjob, subjob
from bigjob_dynamic.many_job import *
from pstar.api.compute.api import PilotCompute, PilotComputeService


from pstar.coordination.advert import AdvertCoordinationAdaptor as CoordinationAdaptor


""" This variable defines the coordination system that is used by BigJob
    e.g. 
        advert://localhost (SAGA/Advert SQLITE)
        advert://advert.cct.lsu.edu:8080 (SAGA/Advert POSTGRESQL)
        redis://localhost:6379 (Redis at localhost)
        tcp://localhost (ZMQ)
"""
COORDINATION_URL = "advert://localhost/?dbtype=sqlite3"


class PilotJobService(PilotJobService):
    """ TROY PilotJobService based on BigJob.
                    
    """

    PJS_ID_PREFIX="pjs-"   

    # Class members
    __slots__ = (
        'id',           # Reference to this PJS
        'url',
        'state',       # Status of the PJS
        'pilot_jobs',    # List of PJs under this PJS
        '__mjs'
    )

    def __init__(self, pjs_url=None):
        """ Create a PilotJobService object.

            Keyword arguments:
            pjs_id -- Don't create a new, but connect to an existing (optional)
        """
        self.__mjs = None
        self.pilot_jobs=[]
        
        if pjs_url==None:      # new pjs          
            self.id = self.PJS_ID_PREFIX+str(uuid.uuid1())
            self.url = "pilotjob://localhost/"+self.id
        else:
            logger.error("Reconnect to PJS currently not supported.")
            

    def create_pilotjob(self, rm=None, pilot_job_description=None, pj_type=None, context=None):
        """ Add a PilotJob to the PilotJobService

            Keyword arguments:
            pilot_job_description -- PilotJob Description
            
            Return value:
            A PilotJob handle
        """
        
        if self.__mjs == None:
            logging.debug("Create Dynamic BigJob Service")            
            self.__mjs = many_job_service([], COORDINATION_URL)
            
        resource_description = self.__translate_pj_bj_description(pilot_job_description)
        bigjob = self.__mjs.add_resource(resource_description)
        pj = PilotJob(self, bigjob, pilot_job_description)
        self.pilot_jobs.append(pj)
        return pj
        
    def __translate_pj_bj_description(self, pilot_job_description):
        resource_description={}
        if pilot_job_description.has_key("service_url"):
            resource_description["resource_url"] = pilot_job_description["service_url"] 
            
        if pilot_job_description.has_key("queue"):
            resource_description["queue"] = pilot_job_description["queue"] 
        else:
            resource_description["queue"] = None
            
        if pilot_job_description.has_key("allocation"):
            resource_description["allocation"] = pilot_job_description["allocation"] 
        else:
            resource_description["allocation"] = None
        
        for i in pilot_job_description.keys():
            resource_description[i] = pilot_job_description[i] 
        
        return resource_description
    
    def list_pilotjobs(self):
        return self.pilot_jobs
        
    
    def cancel(self):
        """ Cancel the PilotJobService.

            This also cancels all the PilotJobs that were under control of this PJS.

            Keyword arguments:
            None

            Return value:
            Result of operation
        """
        self.__mjs.cancel()
        
        
    def _submit_wu(self, work_unit):
        """ Submits work unit to Dynamic Bigjob (ManyJob) 
            Scheduler of Dynamic Bigjob will assign appropriate PJ to WorkUnit        
        """
        subjob = self.__mjs.create_job(work_unit.subjob_description)
        subjob.run()
        work_unit.subjob=subjob
        return work_unit
    
    
    def __repr__(self):
        status_string = "{\n"
        for i in self.pilot_jobs:
            status_string = status_string + " PJ: %s, State: %s;"%(i.get_url(), i.get_state())
        status_string = status_string + "\n}"
        return status_string
    
    
    
class PilotJob(PilotJob):
    """ TROY Wrapper for BigJob class """
     
    def __init__(self, pilot_job_service, bigjob, pilot_job_description):
        logging.debug("Create PilotJob for BigJob: " + str(bigjob))
        self.pilot_job_description=pilot_job_description
        self.__pilot_job_service=pilot_job_service
        self.__bigjob = bigjob        
        self.__subjobs = []
        
    def cancel(self):
        self.__bigjob.cancel()
    
    def get_state(self):
        return self.__bigjob.get_state()
    
    def get_url(self):
        return self.__bigjob.pilot_url
    
    def _submit_wu(self, work_unit):
        """ Submits work unit to Bigjob """
        logging.debug("Submit sub-job to big-job")
        sj = bigjob.bigjob_manager.subjob()
        sj.submit_job(self.__bigjob.pilot_url, work_unit.subjob_description)
        self.__subjobs.append(sj)
        work_unit.subjob=sj
        return work_unit
        
        
        
        