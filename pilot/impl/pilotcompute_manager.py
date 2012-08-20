""" B{PilotCompute Module}: Implementation of L{PilotComputeService}, L{PilotCompute}, L{ComputeUnit}.
"""

import sys
import getopt
import time
import pdb
import os
import traceback
import logging
import uuid


""" import bigjob classes """
from bigjob import logger
from bigjob import bigjob, subjob, description

""" import API objects """
from pilot.api import PilotCompute, PilotComputeService, ComputeUnit, State

""" Configure coordination backend here """
#from pilot.coordination.advert import AdvertCoordinationAdaptor as CoordinationAdaptor
from pilot.coordination.redis_adaptor import RedisCoordinationAdaptor as CoordinationAdaptor


""" This variable defines the coordination system that is used by BigJob
    e.g. 
        advert://localhost (SAGA/Advert SQLITE)
        advert://advert.cct.lsu.edu:8080 (SAGA/Advert POSTGRESQL)
        redis://localhost:6379 (Redis at localhost)
        tcp://localhost (ZMQ)
"""
COORDINATION_URL = "redis://localhost"


class PilotComputeService(PilotComputeService):
    """ B{PilotComputeService}
    
        Factory for L{PilotCompute}s. 
                    
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
        if pcs_url==None:      # new pcs          
            self.id = self.PJS_ID_PREFIX+str(uuid.uuid1())
            self.url = os.path.join(self.coordination_url, "pcs", self.id)
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
           
    
    def list_pilots(self):
        """ List managed L{PilotCompute}s.

            Return value:
            A list of L{PilotCompute} urls

        """
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
    
    
    ###########################################################################
    # Internal methods
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
        
        resource_description["pilot_compute_description"] = pilot_compute_description
        
        return resource_description

    
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

        working_directory = None
        if ("working_directory" in bj_dict):
            working_directory=bj_dict["working_directory"]
            
        bj_filetransfer = None
        if ("file_transfer" in bj_dict):
            bj_filetransfer = bj_dict["file_transfer"]


        bj.start_pilot_job(lrms_url = gram_url,
                           number_nodes = bj_dict["number_of_processes"],
                           queue = bj_dict["queue"],
                           project = bj_dict["allocation"],
                           working_directory = working_directory, 
                           walltime = walltime,
                           processes_per_node = ppn,
                           filetransfers = bj_filetransfer,
                           external_queue = self.coordination_queue,
                           pilot_compute_description = bj_dict["pilot_compute_description"]
                           )
        return bj
    
###############################################################################
    
class PilotCompute(PilotCompute):
    """ B{Pilot Compute} (PC)
    
        This is the object that is returned by the PilotComputeService when a 
        new PilotCompute is created based on a PilotComputeDescription.

        The PilotCompute object can be used by the application to keep track 
        of active pilots.
        
        A ComputePilot has state, can be queried and cancelled.

        Properties:

          - state:            
            The state of the pilot.
         
          - id:
            The id may be 'None' if the Pilot is not yet in Running state.
            The returned ID can be used to connect to the CP instance later 
            on, for example from within a different application instance.  
            type: string (url)

          - pilot_compute_description:
            The PilotComputeDescription used to create this pilot.            
    
    """     
    def __init__(self, pilot_compute_service=None, 
                       bigjob_object=None, 
                       pilot_compute_description=None,
                       pilot_url=None): # for reconnecting
        
        """ Create/reconnect to a Pilot Compute.  

            Keyword arguments:
            pilot_url   -- restore from cp_id

            The implementation will attempt to reconnect to the PC instance
            referenced by the pilot_url.  

        """        
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
        """ Terminates the pilot """
        self.__bigjob.cancel()    
    
    
    def get_state(self):
        """ Returns the state of the pilot """
        return self.__bigjob.get_state()    
    
    
    def wait(self):
        """ Wait until Pilot Compute to enter a final state (Done, Cancel or Failed) 

        It is not an error to call wait() in a final state -- the call simply
        returns immediately.        
        """
        self.__bigjob.wait()
    
    
    def list_compute_units(self):
        """ list managed L{ComputeUnit}s.

            Return value:
            A list of L{ComputeUnit} IDs

            The returned list can include units which have been submitted to
            this pilot.
        """
        sj_list = self.__bigjob.list_subjobs()
        cu_list = []
        for i in sj_list:
            cu_list.append(ComputeUnit(cu_url=i))
        return cu_list
    
    
    def get_url(self):
        """ Get unique URL referencing the Pilot Compute 
            This URL can be used to reconnect to the Pilot Compute        
        """
        return self.__bigjob.get_url()
    
    
    def get_free_nodes(self):
        """ Returns the number of free slots available within the pilot """
        return self.__bigjob.get_free_nodes()
    
    
    def get_details(self):
        """ returns a dict that contains the details of the Pilot Compute, 
                - job state
                - description
                - ...        
        """
        return self.__bigjob.get_details()
       
    
    def submit_compute_unit(self, compute_unit_description):
        """
            Submit a CU to this pilot.

            @param compute_unit_description: The L{ComputeUnitDescription} or dictionary describing
                                             the compute task

            @return: L{ComputeUnit} object

            The CUD is (possibly translated and) passed on to the PDS scheduler,
            which will attempt to instantiate the described workload process on
            the managed set of Pilot Computes.  
            
            On success, the returned CU is in Pending state (or moved into any
            state downstream from Pending).

            The call will will honor all attributes set on the CUD.  Attributes which
            are not explicitly set are interpreted as having default values (see
            documentation of CUD), or, where default values are not specified,
            are ignored.
        """
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
        

###############################################################################
        
class ComputeUnit(ComputeUnit):
    """ ComputeUnit
    
        This is the object that is returned by the ComputeDataService when a 
        new ComputeUnit is submitted based on a ComputeUnitDescription.

        The ComputeUnit object can be used by the application to keep track 
        of ComputeUnits that are active.

        A ComputeUnit has state, can be queried and can be cancelled. 
        
    """
    
    CU_ID_PREFIX="cu-"  

    def __init__(self, compute_unit_description=None, compute_data_service=None, cu_url=None):
        
        if cu_url==None:
            self.id = self.CU_ID_PREFIX + str(uuid.uuid1())
            if compute_data_service!=None:
                self.url = compute_data_service.url + "/" + self.id
                logger.debug("Created CU: %s"%self.url)  
            self.state = State.New       
            self.__subjob = None # reference to BigJob Subjob 
            self.compute_unit_description = compute_unit_description # CU Description
            self.subjob_description = self.__translate_cu_sj_description(compute_unit_description)
        else:
            self.__subjob = subjob(subjob_url=cu_url)
           
    
    def get_id(self):
        return self.id
    
    
    def get_url(self):   
        if self.__subjob!=None:      
            return self.__subjob.get_url()
        else:
            return self.get_id()   
        
    
    def get_details(self):   
        if self.__subjob!=None:      
            return self.__subjob.get_details()
        else:
            return None      
    
    
    def get_state(self):
        if self.__subjob != None:
            self.state = self.__subjob.get_state()
        return self.state
    
    
    def wait(self):
        """ Wait until in Done state 
            (or Failed state)
        """
        while True:
            state = self.get_state()
            logger.debug("Compute Unit: %s, State: %s"%(self.id, state))            
            if state==State.Done or state==State.Failed:
                break
            time.sleep(2)

    
    def cancel(self):
        if self.__subjob != None:
            return self.__subjob.cancel()
        return None
    
    def __repr__(self):
        return self.id

    
    def _update_compute_unit_description(self, compute_unit_description):
        self.compute_unit_description = compute_unit_description # CU Description
        self.subjob_description = self.__translate_cu_sj_description(compute_unit_description)

    def _update_subjob(self, subjob):
        self.__subjob = subjob
        
    # INTERNAL
    def __translate_cu_sj_description(self, compute_unit_description):
        jd = description()
        if compute_unit_description.has_key("executable"): 
            jd.executable = compute_unit_description["executable"]
        if compute_unit_description.has_key("spmd_variation"):
            jd.spmd_variation = compute_unit_description["spmd_variation"]
        else:
            jd.spmd_variation = "single"
        if compute_unit_description.has_key("arguments"): 
            jd.arguments = compute_unit_description["arguments"]
        if compute_unit_description.has_key("environment"):
            jd.environment = compute_unit_description["environment"] 
        
        # handling number of processes
        if compute_unit_description.has_key("number_of_processes"):
            jd.number_of_processes=int(compute_unit_description["number_of_processes"])
        elif compute_unit_description.has_key("total_cpu_count"):
            jd.number_of_processes=int(compute_unit_description["total_cpu_count"])
        else:
            jd.number_of_processes=1
            
        if compute_unit_description.has_key("working_directory"): 
            jd.working_directory = compute_unit_description["working_directory"]
        if compute_unit_description.has_key("output"): 
            jd.output =  compute_unit_description["output"]
        if compute_unit_description.has_key("error"): 
            jd.error = compute_unit_description["error"]
        if compute_unit_description.has_key("file_transfer"):
            jd.file_transfer=compute_unit_description["file_transfer"]  
        if compute_unit_description.has_key("input_data"):
            jd.input_data=compute_unit_description["input_data"]  
        if compute_unit_description.has_key("output_data"):
            jd.output_data=compute_unit_description["output_data"]            
        return jd
        
        
