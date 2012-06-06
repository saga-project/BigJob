import sys
import getopt
import time
import pdb
import os
import traceback
import logging
import uuid

import bigjob

from bigjob import logger
from bigjob import bigjob, subjob
#from bigjob_dynamic.many_job import *

from pilot.api import PilotCompute, PilotComputeService, State
from pilot.impl.pilot_manager import ComputeUnit
#from pilot.coordination.advert import AdvertCoordinationAdaptor as CoordinationAdaptor
from pilot.coordination.nocoord import NoCoordinationAdaptor as CoordinationAdaptor


""" This variable defines the coordination system that is used by BigJob
    e.g. 
        advert://localhost (SAGA/Advert SQLITE)
        advert://advert.cct.lsu.edu:8080 (SAGA/Advert POSTGRESQL)
        redis://localhost:6379 (Redis at localhost)
        tcp://localhost (ZMQ)
"""
COORDINATION_URL = "advert://localhost/?dbtype=sqlite3"


class PilotComputeService(PilotComputeService):
    """ PilotJobService based on BigJob.
                    
    """

    PJS_ID_PREFIX="pcs-"   

    # Class members
    __slots__ = (
        'id',           # Reference to this PJS
        'url',
        'state',       # Status of the PJS
        'pilot_computes',    # List of PJs under this PJS
        'coordination_url'
    )

    def __init__(self, coordination_url=COORDINATION_URL, pjs_url=None):
        """ Create a PilotJobService object.

            Keyword arguments:
            pjs_id -- Don't create a new, but connect to an existing (optional)
        """
        self.__mjs = None
        self.pilot_computes=[]
        self.coordination_url=coordination_url
        
        if pjs_url==None:      # new pjs          
            self.id = self.PJS_ID_PREFIX+str(uuid.uuid1())
            self.url = "pilotjob://localhost/"+self.id
        else:
            logger.error("Reconnect to PJS currently not supported.")
            

    def create_pilot(self, rm=None, pilot_compute_description=None, pj_type=None, context=None):
        """ Add a PilotJob to the PilotJobService

            Keyword arguments:
            pilot_compute_description -- PilotJob Description
            
            Return value:
            A PilotJob handle
        """
        
        if self.__mjs == None:
            logging.debug("Create Dynamic BigJob Service")            
            #self.__mjs = many_job_service([], self.coordination_url)
            
        bj_dict = self.__translate_pj_bj_description(pilot_compute_description)
        bj = self.__start_bigjob(bj_dict)
        
        #bigjob = self.__mjs.add_resource(resource_description)
        pj = PilotCompute(self, bj, pilot_compute_description)
        self.pilot_computes.append(pj)
        return pj
        
    def __translate_pj_bj_description(self, pilot_compute_description):
        resource_description={}
        if pilot_compute_description.has_key("service_url"):
            resource_description["resource_url"] = pilot_compute_description["service_url"] 
            
        if pilot_compute_description.has_key("queue"):
            resource_description["queue"] = pilot_compute_description["queue"] 
        else:
            resource_description["queue"] = None
            
        if pilot_compute_description.has_key("allocation"):
            resource_description["allocation"] = pilot_compute_description["allocation"] 
        else:
            resource_description["allocation"] = None
        
        for i in pilot_compute_description.keys():
            resource_description[i] = pilot_compute_description[i] 
        
        return resource_description
    
    def list_pilots(self):
        return self.pilot_computes
        
    
    def cancel(self):
        """ Cancel the PilotJobService.

            This also cancels all the PilotJobs that were under control of this PJS.

            Keyword arguments:
            None

            Return value:
            Result of operation
        """
        pass
        #self.__mjs.cancel()
        
        
#    def submit_cu(self, compute_unit):
#        """ Submits work unit to Dynamic Bigjob (ManyJob) 
#            Scheduler of Dynamic Bigjob will assign appropriate PJ to WorkUnit        
#        """
#        subjob = self.__mjs.create_job(compute_unit.subjob_description)
#        subjob.run()
#        compute_unit.subjob=subjob
#        return compute_unit
    
    
    def __repr__(self):
        status_string = "{\n"
        for i in self.pilot_computes:
            status_string = status_string + " PJ: %s, State: %s;"%(i.get_url(), i.get_state())
        status_string = status_string + "\n}"
        return status_string
    
    
    def __start_bigjob(self, bj_dict):
        """ private method - starts a bigjob on the defined resource """
        gram_url = bj_dict["resource_url"]
        logging.debug("start bigjob at: " + gram_url)
        bj = bigjob(self.coordination_url)
        ppn="1"
        if ("processes_per_node" in bj_dict):
            ppn=bj_dict["processes_per_node"]
        else:
            bj_dict["processes_per_node"]="1"

        walltime = 3600
        if ("walltime" in bj_dict):
            walltime=bj_dict["walltime"]

        working_directory = (os.getcwd()+"/agent")
        if ("working_directory" in bj_dict):
            working_directory=bj_dict["working_directory"]
            
        bj_filetransfer = None
        if ("file_transfer" in bj_dict):
            bj_filetransfer = bj_dict["file_transfer"]

        bj.start_pilot_job(gram_url,
                           None,
                           bj_dict["number_of_processes"],
                           bj_dict["queue"],
                           bj_dict["allocation"],
                           working_directory, 
                           None,
                           walltime,
                           ppn,
                           filetransfers=bj_filetransfer)
        return bj
    
class PilotCompute(PilotCompute):
    """ Wrapper for BigJob class """
     
    def __init__(self, pilot_compute_service=None, 
                       bigjob=None, 
                       pilot_compute_description=None,
                       pilot_url=None):
        if pc_id==None:
            logging.debug("Create PilotCompute for BigJob: " + str(bigjob))
            self.pilot_compute_description=pilot_compute_description
            self.__pilot_compute_service=pilot_compute_service
            self.__bigjob = bigjob        
            self.__subjobs = []
        else:
            logging.debug("Reconnect to an existing Pilot Compute")
            self.__bigjob = bigjob(pilot_url=pilot_url)
            
        
    def cancel(self):
        self.__bigjob.cancel()    
    
    def get_state(self):
        return self.__bigjob.get_state()    
    
    def wait(self):
        """ Waits for completion of CUs """
        self.__bigjob.wait()
    
    def get_url(self):
        #return self.__bigjob.pilot_url
        return self.__bigjob.get_url()
        
    def get_free_nodes(self):
        return self.__bigjob.get_free_nodes()
    
    def submit_cu(self, compute_unit):
        """ Submits compute unit to Bigjob """
        logging.debug("Submit CU to big-job")
        sj = subjob()
        sj.submit_job(self.__bigjob.pilot_url, compute_unit.subjob_description)
        self.__subjobs.append(sj)
        compute_unit.subjob=sj
        return compute_unit
    
    def submit_compute_unit(self, compute_unit_description):
        """ Submits work unit to Bigjob """
        cu = ComputeUnit(compute_unit_description)
        return self.submit_cu(cu)
    
    def __repr__(self):
        return str(self.__bigjob)
        
        
        
        
