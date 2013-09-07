""" B{ComputeDataServiceDecentral Module}: A decentral implementation of a ComputeDataService (see L{ComputeDataServiceDecentral}).

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

import bigjob
from bigjob import logger, bigjob, subjob, description

import pilot
from pilot.api import ComputeDataService, State
from pilot.api.api import PilotError
from pilot.impl.pilotdata_manager import PilotData, DataUnit
from pilot.impl.pilotcompute_manager import PilotCompute, PilotComputeService
from pilot.impl.pilot_manager import ComputeUnit


#from pilot.coordination.advert import AdvertCoordinationAdaptor as CoordinationAdaptor
from pilot.coordination.redis_adaptor import RedisCoordinationAdaptor as CoordinationAdaptor

""" Loaded Module determines scheduler:
    
    bigdata.scheduler.data_compute_scheduler - selects random locations for PD and CUs
    bigdata.scheduler.data_compute_affinity_scheduler - considers affinity descriptions
    
"""
from pilot.scheduler.data_compute_affinity_scheduler import Scheduler

class ComputeDataServiceDecentral(ComputeDataService):
    """ B{ComputeDataServiceDecentral.}
    
        The ComputeDataService is the application's interface to submit 
        ComputeUnits and PilotData/DataUnit to the Pilot-Manager 
        in the P* Model.
        
        The decentral ComputeDateService will only work with Redis!
    """    
    CDS_ID_PREFIX="cds-"  

    def __init__(self, cds_url=None):
        """ Create a ComputeDataService (Decentral) object.

            @param cds_url: Reconnect to an existing CDS (optional).
        """
        # Pilot Data
        self.data_units={}
        self.pilot_data_services=[]
        
        # Pilot Compute
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
        self.du_queue = Queue.Queue()
        
        self.stop=threading.Event()
        self.scheduler_thread=threading.Thread(target=self._scheduler_thread)
        self.scheduler_thread.daemon=True
        self.scheduler_thread.start()
        logger.debug("Created ComputeDataServiceDecentral")
           
        
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
        if len(self.pilot_job_services)>1:
            logger.error("Decentral ComputeDataService only supports 1 PilotComputeService")
            raise PilotError("Decentral ComputeDataService only supports 1 PilotComputeService")
        

    def remove_pilot_compute_service(self, pcs):
        """ Remove a PilotJobService from this CDS.

            Note that it won't cancel the PilotJobService, it will just no
            longer be connected to this WUS.

            Keyword arguments:
            pilotjob_services -- The PilotJob Service(s) to remove from this
                                 Work Unit Service. 

            Return:
            Result
        """
        self.pilot_job_services.remove(pcs)
        CoordinationAdaptor.update_cds(self.url, self)
        if len(self.pilot_job_services)>1:
            logger.error("Decentral ComputeDataService only supports 1 PilotComputeService")
            raise PilotError("Decentral ComputeDataService only supports 1 PilotComputeService")


    def submit_compute_unit(self, compute_unit_description):
        """ Submit a CU to this Compute Data Service.
            @param compute_unit_description:  The L{ComputeUnitDescription} from the application
            @return: L{ComputeUnit} object
        """
        cu = ComputeUnit(compute_unit_description, self)
        self.compute_units[cu.id]=cu
        self.__submit_cu(cu)        
        return cu
    
    
    def list_pilot_compute(self):
        """ List all pilot compute of CDS """
        return self.pilot_job_service
    
    
    def get_details(self):
        """ returns a list with dicts that contains the details of the Pilot Compute, 
                - job state
                - description
                - ...        
        """
        pilot_details=[]
        for pcs in self.pilot_job_services:
            for pc in pcs.list_pilots():
                pilot_details.append(pc.get_details())
        return pilot_details
    
    ###########################################################################
    # Compute Data Service private methods
    def __submit_cu(self, compute_unit):
        """ Submits compute unit to Bigjob """
                
        if len(self.pilot_job_services)!=1:
            raise PilotError("No PilotComputeService found. Please start a PCS before submitting ComputeUnits.")
        
        self.__wait_for_du(compute_unit)
        
        sj = subjob()
        self.pcs_coordination_namespace=self.pilot_job_services[0].coordination_queue
        logger.debug("Submit CU to big-job via external queue: %s"%self.pcs_coordination_namespace)
        sj.submit_job(self.pcs_coordination_namespace, compute_unit.subjob_description)

        compute_unit._update_subjob(sj)
        return compute_unit
    
    
    def __wait_for_du(self, compute_unit):
        """ wait for Data Units that are required for Compute Unit """
        cu_description = compute_unit.compute_unit_description
        if cu_description.has_key("input_data") and len(cu_description["input_data"])>0:
            for input_du_url in cu_description["input_data"]:
                for du in self.data_units.values():
                    if input_du_url == du.get_url():
                        logger.debug("Wait for DU: %s"%du.get_url())
                        du.wait()
    
    
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
    
    
    ###########################################################################
    # General     
   
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
            dus = self.data_units.values()
            cus = self.compute_units.values()
            pilots = []
            for i in self.pilot_job_services:
                pilots.extend(i.list_pilots())
            number_dus=len(dus)
            number_cus=len(cus)
            number_pilots=len(pilots)
            completed_dus=0
            completed_cus=0
            completed_pilots=0
            logger.debug("### ComputeDataService wait for completion of %d CUs/ %d DUs ###"%(len(cus), len(dus)))
            
            while not (completed_dus==number_dus and completed_cus==number_cus):
                completed_dus=0
                completed_cus=0
                completed_pilots=0
            
                for p in pilots:
                    state = p.get_state()
                    if state==State.Done or state==State.Failed:
                        completed_pilots = completed_pilots + 1
                        
                if  completed_pilots==number_pilots:
                    logger.debug("All pilots done/failed. No more active pilots. Exit.")
                    break

                for cu in cus:
                    state = cu.get_state()
                    if state==State.Done or state==State.Failed:
                        completed_cus=completed_cus + 1

                for du in dus:
                    state = du.get_state()
                    if state==State.Running or state==State.Failed:
                        completed_dus=completed_dus + 1

                logger.debug("Compute Data Service Completion Status: %d/%d CUs %d/%d DUs %d/%d Pilots"%
                             (completed_cus, number_cus, completed_dus,
                               number_dus, completed_pilots, number_pilots))
            
                logger.debug("exit? " + str((completed_dus==number_dus and completed_cus==number_cus)))
                if completed_dus<number_dus and completed_cus<number_cus:
                    time.sleep(2)
            
            
#             for i in self.data_units.values():
#                 i.wait()
#             logger.debug("Wait for DUs finished")        
#                 
#             for i in self.compute_units.values():
#                 i.wait()     
#             logger.debug("CUs done")        
                   
            logger.debug("### END WAIT ###")
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print "*** print_tb:"
            traceback.print_tb(exc_traceback, limit=1, file=sys.stderr)
            print "*** print_exception:"
            traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stderr)
            logger.debug("Ctrl-c detected. Terminating ComputeDataService...")
            self.cancel()
            raise KeyboardInterrupt
                
        
    def get_state(self):
        return self.state
    
    
    def get_id(self):
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
                        #logger.debug("Transfer to PD finished.")
                        #du._update_state(State.Running) 
                        #logger.debug("Updated State to Running.")
                        self.du_queue.task_done()                   
                    else:
                        self.du_queue.task_done() 
                        self.du_queue.put(du)
            except Queue.Empty:
                pass
               
            if self.du_queue.empty():
                time.sleep(5)        

        logger.debug("Re-Scheduler terminated")
    
    
   
    
   