"""
B{ComputeDataService Module}: A central implementation of the L{ComputeDataService}

A Meta-Scheduling service for pilots (both PilotCompute and PilotData)

"""

import sys
import os
import time
import threading
import logging
import pdb
import Queue
import uuid
import traceback
import urlparse

from bigjob import logger

import pilot
from pilot.api import ComputeDataService, State
from pilot.impl.pilotdata_manager import DataUnit
from pilot.impl.pilotcompute_manager import ComputeUnit


#from pilot.coordination.advert import AdvertCoordinationAdaptor as CoordinationAdaptor
#from pilot.coordination.nocoord_adaptor import NoCoordinationAdaptor as CoordinationAdaptor
from pilot.coordination.redis_adaptor import RedisCoordinationAdaptor as CoordinationAdaptor

""" Loaded Module determines scheduler:
    
    bigdata.scheduler.data_compute_scheduler - selects random locations for PD and CUs
    bigdata.scheduler.data_compute_affinity_scheduler - considers affinity descriptions
    
"""
from pilot.scheduler.data_compute_affinity_scheduler import Scheduler

class ComputeDataService(ComputeDataService):
    """ B{ComputeDataService (CDS).}
    
        The ComputeDataService is the application's interface to submit 
        ComputeUnits and PilotData/DataUnit to the Pilot-Manager 
        in the P* Model.
    """    
    CDS_ID_PREFIX="cds-"  


    def __init__(self, cds_url=None):
        """ Create a ComputeDataService object.

            Keyword arguments:
            cds_url -- Reconnect to an existing CDS (optional).
        """
        # Pilot Data
        self.data_units={}
        self.pilot_data_services=[]
        
        # Pilot Job
        self.compute_units={}
        self.pilot_job_services=[]
            
        if cds_url == None:
            self.id=self.CDS_ID_PREFIX + str(uuid.uuid1())
            application_url = CoordinationAdaptor.get_base_url(pilot.application_id)
            self.url = CoordinationAdaptor.add_cds(application_url, self)            
        else:
            self.id = self.__get_cds_id(cds_url)
            self.url = cds_url
           
        # Background Thread for scheduling
        self.scheduler = Scheduler()
        self.cu_queue = Queue.Queue()
        self.du_queue = Queue.Queue()
        self.stop=threading.Event()
        self.scheduler_thread=threading.Thread(target=self._scheduler_thread)
        self.scheduler_thread.daemon=True
        self.scheduler_thread.start()
        

    def __get_cds_id(self, cds_url):
        start = cds_url.index(self.CDS_ID_PREFIX)
        end =cds_url.index("/", start)
        return cds_url[start:end]


    ###########################################################################
    # Pilot Compute    
    def add_pilot_compute_service(self, pcs):
        """ Add a PilotComputeService to this CDS.

            @param pcs: The PilotComputeService to which this ComputeDataService will connect.

        """
        self.pilot_job_services.append(pcs)
        CoordinationAdaptor.update_cds(self.url, self)
        

    def remove_pilot_compute_service(self, pcs):
        """ Remove a PilotJobService from this CDS.

            Note that it won't cancel the PilotComputeService, it will just no
            longer be connected to this CDS.

            Keyword arguments:
            @param pcs: The PilotComputeService to remove from this ComputeDataService. 
        """
        self.pilot_job_services.remove(pcs)
        CoordinationAdaptor.update_cds(self.url, self)


    def submit_compute_unit(self, compute_unit_description):
        """ Submit a CU to this Compute Data Service.

            @param compute_unit_description: The ComputeUnitDescription from the application
            @return: ComputeUnit object
        """
        cu = ComputeUnit(compute_unit_description, self)
        self.compute_units[cu.id]=cu
        self.cu_queue.put(cu)
        CoordinationAdaptor.update_cds(self.url, self)
        return cu
    
    def list_pilot_compute(self):
        """ List all pilot compute of CDS """
        return self.pilot_job_service
    
    ###########################################################################
    # Pilot Data     
    def add_pilot_data_service(self, pds):
        """ Add a PilotDataService 

            @param pds: The PilotDataService to add.
        """
        self.pilot_data_services.append(pds)
        CoordinationAdaptor.update_cds(self.url, self)
    
    def remove_pilot_data_service(self, pds):
        """ Remove a PilotDataService 
            @param pds: The PilotDataService to remove 
        """
        self.pilot_data_services.remove(pds)
        CoordinationAdaptor.update_cds(self.url, self)
    
     
    def list_pilot_data(self):
        """ List all pilot data of CDS """
        return self.pilot_data_services
    
    
    def list_data_units(self):
        """ List all DUs of CDS """
        return self.data_units.items()
    
    
    def get_data_unit(self, du_id):
        if self.data_units.has_key(du_id):
            return self.data_units[du_id]
        return None
    
    
    def submit_data_unit(self, data_unit_description):
        """ creates a data unit object and binds it to a physical resource (a pilotdata) """
        du = DataUnit(pilot_data=None, 
                      data_unit_description=data_unit_description)
        self.data_units[du.id]=du
        self.du_queue.put(du)
        # queue currently not persisted
        CoordinationAdaptor.update_cds(self.url, self)
        return du
    
    
    def cancel(self):
        """ Cancel the CDS. 
            All associated PD and PC objects are canceled.            
        """
        # terminate background thread
        self.stop.set()
        CoordinationAdaptor.delete_cds(self.url)
   
    def wait(self):
        """ Waits for CUs and DUs. Return after all DU's have been placed (i.e. in state Running)
            and all CU's have been completed (i.e. in state Done) or if a fault has occurred or
            the user has cancelled a CU or DU.            
        """
        try:
            logger.debug("### START WAIT ###")
            self.cu_queue.join()
            logger.debug("CU queue empty")        
            self.du_queue.join()
            logger.debug("DU queue empty")        
    
            for i in self.data_units.values():
                i.wait()
            logger.debug("DUs done")        
                
            for i in self.compute_units.values():
                i.wait()     
            logger.debug("CUs done")        
                   
            logger.debug("### END WAIT ###")
        except:
            logger.debug("Ctrl-c detected. Terminating ComputeDataService...")
            self.cancel()
            raise KeyboardInterrupt
                
        
    def get_state(self):
        "@return: State of the ComputeDataService"
        return self.state
    
    
    def get_id(self):
        "@return: id of ComputeDataService"
        return str(self.id)
    
    
    def __del__(self):
        """ Make sure that background thread terminates"""
        self.cancel()
   
    ###########################################################################
    # Internal Scheduling
    def __update_scheduler_resources(self):
        logger.debug("__update_scheduler_resources")        
        pd = [s for i in self.pilot_data_services for s in i.list_pilots()]
        self.scheduler.set_pilot_data(pd)
        pj = [p for i in self.pilot_job_services for p in i.list_pilots()]
        logger.debug("Pilot-Jobs: " + str(pj))
        self.scheduler.set_pilot_jobs(pj)
    
    def _schedule_du(self, du):
        """ Schedule DU to a suitable pilot data
        
            Currently one level of scheduling is used:
                1.) Add all resources managed by the contained PDS 
                2.) Select one resource
        """ 
        logger.debug("Schedule PD")
        self.__update_scheduler_resources()
        selected_pilot_data = self.scheduler.schedule_pilot_data(du.data_unit_description)
        return selected_pilot_data 
    
    def _schedule_cu(self, cu):
        logger.debug("Schedule CU")
        self.__update_scheduler_resources()
        selected_pilot_job = self.scheduler.schedule_pilot_job(cu.compute_unit_description)
        return selected_pilot_job
    
    def _scheduler_thread(self):
        while True and self.stop.isSet()==False:            
            try:
                #logger.debug("Scheduler Thread: " + str(self.__class__) + " Pilot Data")
                du = self.du_queue.get(True, 1)  
                # check whether this is a real du object  
                if isinstance(du, DataUnit):
                    pd=self._schedule_du(du)                
                    if(pd!=None):                        
                        logger.debug("Initiate Transfer to PD.")
                        du.add_pilot_data(pd)
                        logger.debug("Transfer to PD finished.")
                        du._update_state(State.Running) 
                        self.du_queue.task_done()                   
                    else:
                        self.du_queue.task_done() 
                        self.du_queue.put(du)
            except Queue.Empty:
                pass
                    
            try:    
                #logger.debug("Scheduler Thread: " + str(self.__class__) + " Pilot Job")
                cu = self.cu_queue.get(True, 1)                
                if isinstance(cu, ComputeUnit):  
                    self.__wait_for_du(cu)                  
                    pj=self._schedule_cu(cu) 
                    if pj !=None:
                        cu = self.__expand_working_directory(cu, pj)                        
                        pj._submit_cu(cu)           
                        self.cu_queue.task_done()         
                    else:
                        logger.debug("No resource found.")
                        self.cu_queue.task_done() 
                        self.cu_queue.put(cu)
            except Queue.Empty:
                pass
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                logger.error("*** print_tb:")
                traceback.print_tb(exc_traceback, limit=1, file=sys.stderr)
                logger.error("*** print_exception:")
                traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stderr)
            
            if self.cu_queue.empty() and self.du_queue.empty():
                time.sleep(5)        

        logger.debug("Re-Scheduler terminated")
    
   
    def __wait_for_du(self, compute_unit):
        """ wait for Data Units that are required for Compute Unit """
        cu_description = compute_unit.compute_unit_description
        if cu_description.has_key("input_data") and len(cu_description["input_data"])>0:
            for input_du_url in cu_description["input_data"]:
                for du in self.data_units.values():
                    if input_du_url == du.get_url():
                        logger.debug("Wait for DU: %s"%du.get_url())
                        du.wait()      
    
    def __expand_working_directory(self, compute_unit, pilot_job):
        """ 
            DEPRECATED capability!
            Expand pilotdata:// url specified in the compute_unit_description 
            to a local url on the machine of the PJ
            
            pilotdata://localhost/434bfc5c-23fd-11e1-a43f-00264a13ca4c
            
            to
            
           /tmp/pilotstore//434bfc5c-23fd-11e1-a43f-00264a13ca4c on machine running pilot_job        
        """ 
        #=======================================================================
        # if compute_unit.compute_unit_description.has_key("working_directory"):
        #    working_directory=compute_unit.compute_unit_description["working_directory"]       
        #    if working_directory.find(DataUnit.DU_ID_PREFIX)!=-1:
        #        pilot_data_url = working_directory
        #        pj_description = pilot_job.pilot_compute_description
        #        pj_dc_affinity = pj_description["affinity_datacenter_label"]
        #        pj_machine_affinity = pj_description["affinity_machine_label"]
        #        pd = [s for i in self.pilot_data_services for s in i.list_pilots()]
        #        
        #        # find all pilot stores with the same affinity
        #        candidate_pd = []
        #        for i in pd:
        #            pd_description = i.pilot_data_description
        #            pd_dc_affinity = pd_description["affinity_datacenter_label"]
        #            pd_machine_affinity = pd_description["affinity_machine_label"]
        #            if pd_dc_affinity == pj_dc_affinity and pd_machine_affinity == pj_machine_affinity:
        #                candidate_pd.append(i)
        #            
        #        # check whether required pilot_data is part of pilot_data
        #        target_pd = None  
        #        target_du = None  
        #        for pd in candidate_pd:
        #            for du in pd.list_data_units():
        #                logger.debug("DU URL:%s"%(du.url))
        #                if du.url == pilot_data_url:
        #                    logger.debug("Found PD %s at %s"%(du.url, pd.service_url))
        #                    target_pd = pd 
        #                    target_du = du
        #                    break
        #        if target_du == None:
        #            self.__stage_du_to_pj(pilot_data_url, pilot_job)
        #        
        #        if target_pd!=None:
        #            pd_url = target_pd.url_for_du(target_du)
        #            components = urlparse.urlparse(pd_url)
        #            compute_unit.compute_unit_description["working_directory"] = components.path
        #            compute_unit._update_compute_unit_description(compute_unit.compute_unit_description)
        #            logger.debug("__expand_working_directory %s: Set working directory to %s"%(pilot_data_url, compute_unit.compute_unit_description["working_directory"]))
        #            return compute_unit
        # 
        #=======================================================================
        return compute_unit
            
            
    def __stage_du_to_pj(self, pilotdata, pilotjob):
        """
            stage required files to machine of pilot job
        """
        pass
    
    def __find_pd_at_pj_resource(self, pilotjob):
        pass
   

###############################################################################
# Unimplemented entities

class ComputeUnitService(object):
    def __init__(self):
        raise NotImplementedError("Please use ComputeDataService.")


class DataUnitService(object):
    def __init__(self):
        raise NotImplementedError("Please use ComputeDataService.")
    

