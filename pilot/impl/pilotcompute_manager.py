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
    """ PilotComputeService based on BigJob.
                    
    """
    PJS_ID_PREFIX="pcs-"   


    def __init__(self, coordination_url=COORDINATION_URL, pcs_url=None):
        """ Create a PilotJobService object.

            Keyword arguments:
            pcs_id -- Don't create a new, but connect to an existing (optional)
        """
        self.pilot_computes=[]
        self.coordination_url=coordination_url
        self.coordination_queue=""
        if pcs_url==None:      # new pjs          
            self.id = self.PJS_ID_PREFIX+str(uuid.uuid1())
            self.url = os.path.join(self.coordination_url, "pcs", self.id)
            # self.coordination_queue = self.id
            self.coordination_queue = "PilotComputeServiceQueue-" + str(self.id)
            logger.debug("Created Pilot Compute Service: %s"%self.url)
        else:
            logger.error("Reconnect to PilotComputeService currently not supported.")
        
            

    def create_pilot(self, rm=None, pilot_compute_description=None, pj_type=None, context=None):
        """ Add a PilotJob to the PilotJobService

            Keyword arguments:
            pilot_compute_description -- PilotJob Description
            
            Return value:
            A PilotCompute object
        """
        bj_dict = self.__translate_pj_bj_description(pilot_compute_description)
        bj = self.__start_bigjob(bj_dict)
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
        """ Cancel the PilotComputeService.

            This also cancels all the PilotJobs that were under control of this PJS.

            Keyword arguments:
            None

            Return value:
            Result of operation
        """
        for i in self.pilot_computes:
            i.cancel()
            
    
    def __repr__(self):
        status_string = "{\n"
        for i in self.pilot_computes:
            status_string = status_string + " PJ: %s, State: %s;"%(i.get_url(), i.get_state())
        status_string = status_string + "\n}"
        return status_string
    
    
    def __start_bigjob(self, bj_dict):
        """ private method - starts a bigjob on the defined resource """
        gram_url = bj_dict["resource_url"]
        logger.debug("start bigjob at: " + gram_url)
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
                           filetransfers=bj_filetransfer,
                           external_queue=self.coordination_queue)
        return bj
    
    
class PilotCompute(PilotCompute):
    """ Wrapper for BigJob class """
     
    def __init__(self, pilot_compute_service=None, 
                       bigjob_object=None, 
                       pilot_compute_description=None,
                       pilot_url=None): # for reconnecting
        
        self.__subjobs = []
        self.__pilot_compute_service = None
        if pilot_url==None:
            logger.debug("Create PilotCompute for BigJob: " + str(bigjob_object))
            self.pilot_compute_description=pilot_compute_description
            self.__pilot_compute_service=pilot_compute_service
            self.__bigjob = bigjob_object        
        else:
            logger.debug("Reconnect to an existing Pilot Compute")
            self.__bigjob = bigjob(pilot_url=pilot_url)
        
        # Store the URL of pilot compute service for later reference
        # This URL is used as central queue for a set of BJs in the
        # ComputeDataServiceDecentral
        if self.__pilot_compute_service!=None:
            self.coordination_queue = pilot_compute_service.coordination_queue
            
        
    def cancel(self):
        self.__bigjob.cancel()    
    
    
    def get_state(self):
        return self.__bigjob.get_state()    
    
    
    def wait(self):
        """ Waits for completion of CUs """
        self.__bigjob.wait()
    
    
    def list_cus(self):
        """ Returns a list of compute units managed by the pilot """
        sj_list = self.__bigjob.list_subjobs()
        cu_list = []
        for i in sj_list:
            cu_list.append(ComputeUnit(cu_url=i))
        return cu_list
    
    
    def get_url(self):
        """ Get unique URL referencing the Pilot Compute """
        return self.__bigjob.get_url()
    
    
    def get_free_nodes(self):
        return self.__bigjob.get_free_nodes()
    
    
    def get_details(self):
        return self.__bigjob.get_details()
       
    
    def submit_compute_unit(self, compute_unit_description):
        """ Submits work unit to Bigjob """
        cu = ComputeUnit(compute_unit_description)
        return self._submit_cu(cu)
    
    
    def __repr__(self):
        return str(self.__bigjob)
        
        
    ###########################################################################
    # Internal methods
        
    def _submit_cu(self, compute_unit):
        """ Submits compute unit to Bigjob """
        logger.debug("Submit CU to big-job")
        sj = subjob()
        sj.submit_job(self.__bigjob.pilot_url, compute_unit.subjob_description)
        self.__subjobs.append(sj)
        compute_unit._update_subjob(sj)
        return compute_unit
        
        
        
