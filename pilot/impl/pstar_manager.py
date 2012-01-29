import sys
import os
import time
import threading
import logging
import pdb
import Queue
import saga
import uuid
import traceback
import urlparse

import bigjob
import pstar
from bigjob import logger
from pstar.api.compute.api import WorkUnit, State
from pstar.impl.pilotdata_manager import PilotData
from pstar.coordination.advert import AdvertCoordinationAdaptor as CoordinationAdaptor

""" Loaded Module determines scheduler:
    
    bigdata.scheduler.data_compute_scheduler - selects random locations for PD and WUs
    bigdata.scheduler.data_compute_affinity_scheduler - considers affinity descriptions
    
"""

from pstar.scheduler.data_compute_affinity_scheduler import Scheduler

class WorkDataService(object):
    """ TROY WorkDataService.
    
        The WorkDataService is the application's interface to submit 
        WorkUnits and PilotData/DataUnit to the Pilot-Manager 
        in the P* Model.
    """    
    WDS_ID_PREFIX="wds-"  


    def __init__(self, wds_url=None):
        """ Create a Work Data Service object.

            Keyword arguments:
            wds_url -- Reconnect to an existing WDS (optional).
        """
        # Pilot Data
        self.pilot_data={}
        self.pilot_store_services=[]
        
        # Pilot Job
        self.pilot_job_services=[]
        self.work_units={}
            
        if wds_url == None:
            self.id=self.WDS_ID_PREFIX + str(uuid.uuid1())
            application_url = CoordinationAdaptor.get_base_url(pstar.application_id)
            self.url = CoordinationAdaptor.add_wds(application_url, self)
            
        else:
            self.id = self.__get_wds_id(wds_url)
            self.url = wds_url
           
        # Background Thread for scheduling
        self.scheduler = Scheduler()
        self.wu_queue = Queue.Queue()
        self.pd_queue = Queue.Queue()
        self.stop=threading.Event()
        self.scheduler_thread=threading.Thread(target=self._scheduler_thread)
        self.scheduler_thread.start()

    def __get_wds_id(self, wds_url):
        start = wds_url.index(self.WDS_ID_PREFIX)
        end =wds_url.index("/", start)
        return wds_url[start:end]


    ###########################################################################
    # Pilot Job
    
    def add_pilot_job_service(self, pjs):
        """ Add a PilotJobService to this WUS.

            Keyword arguments:
            pilotjob_services -- The PilotJob Service(s) to which this 
                                 Work Unit Service will connect.

            Return:
            Result
        """
        self.pilot_job_services.append(pjs)
        CoordinationAdaptor.update_wds(self.url, self)

    def remove_pilot_job_service(self, pjs):
        """ Remove a PilotJobService from this WUS.

            Note that it won't cancel the PilotJobService, it will just no
            longer be connected to this WUS.

            Keyword arguments:
            pilotjob_services -- The PilotJob Service(s) to remove from this
                                 Work Unit Service. 

            Return:
            Result
        """
        self.pilot_job_services.remove(pjs)
        CoordinationAdaptor.update_wds(self.url, self)

    def submit_work_unit(self, work_unit_description):
        """ Submit a WU to this Work Unit Service.

            Keyword argument:
            wud -- The WorkUnitDescription from the application

            Return:
            WorkUnit object
        """
        wu = WorkUnit(work_unit_description, self)
        self.work_units[wu.id]=wu
        self.wu_queue.put(wu)
        CoordinationAdaptor.update_wds(self.url, self)
        return wu
    
    ###########################################################################
    # Pilot Data 
    
    def add_pilot_store_service(self, pss):
        """ Add a PilotStoreService 

            Keyword arguments:
            pss -- The PilotStoreService to add.

            Return:
            None
        """
        self.pilot_store_services.append(pss)
        CoordinationAdaptor.update_wds(self.url, self)
    
    def remove_pilot_store_service(self, pss):

        """ Remove a PilotStoreService 
            
            Keyword arguments:
            pss -- The PilotStoreService to remove 
            
            Return:
            None
        """
        self.pilot_store_services.remove(pss)
        CoordinationAdaptor.update_wds(self.url, self)
    
    def list_pilotstores(self):
        """ List all PDs of PDS """
        return self.pilot_store_services
    
    
    def list_pilotdata(self):
        """ List all PDs of PDS """
        return self.pilot_data.items()
    
    
    def get_pilotdata(self, pd_id):
        if self.pilot_data.has_key(pd_id):
            return self.pilot_data[pd_id]
        return None
    
    
    def submit_pilot_data(self, pilot_data_description):
        """ creates a pilot data object and binds it to a physical resource (a pilotstore) """
        pd = PilotData(pilot_data_service=self, 
                       pilot_data_description=pilot_data_description)
        self.pilot_data[pd.id]=pd
        self.pd_queue.put(pd)
        # queue currently not persisted
        CoordinationAdaptor.update_wds(self.url, self)
        return pd
    
    def cancel(self):
        """ Cancel the PDS. 
            All associated PD objects are deleted and removed from the associated pilot stores.            
            
            Keyword arguments:
            None

            Return:
            None
        """
        # terminate background thread
        self.stop.set()
        CoordinationAdaptor.delete_wds(self.url)
   
    def get_state(self):
        return self.state
    
    
    def get_id(self):
        return str(self.id)
   
    ###########################################################################
    # Internal Scheduling
    def __update_scheduler_resources(self):
        logging.debug("__update_scheduler_resources")        
        ps = [s for i in self.pilot_store_services for s in i.list_pilotstores()]
        self.scheduler.set_pilot_stores(ps)
        pj = [p for i in self.pilot_job_services for p in i.list_pilotjobs()]
        logging.debug("Pilot-Jobs: " + str(pj))
        self.scheduler.set_pilot_jobs(pj)
    
    def _schedule_pd(self, pd):
        """ Schedule PD to a suitable pilot store
        
            Currently one level of scheduling is used:
                1.) Add all resources managed by PSS of this PSS
                2.) Select one resource
        """ 
        logging.debug("Schedule PD")
        self.__update_scheduler_resources()
        selected_pilot_store = self.scheduler.schedule_pilot_data(pd.pilot_data_description)
        return selected_pilot_store 
    
    def _schedule_wu(self, wu):
        logging.debug("Schedule PD")
        self.__update_scheduler_resources()
        selected_pilot_job = self.scheduler.schedule_pilot_job(wu.work_unit_description)
        return selected_pilot_job
    
    def _scheduler_thread(self):
        while True and self.stop.isSet()==False:            
            try:
                logging.debug("Scheduler Thread: " + str(self.__class__) + " Pilot Data")
                pd = self.pd_queue.get(True, 1)  
                # check whether this is a real pd object  
                if isinstance(pd, PilotData):
                    ps=self._schedule_pd(pd)                
                    if(ps!=None):                        
                        logging.debug("Transfer to PS finished.")
                        pd.add_pilot_store(ps)
                        pd.update_state(State.Running)                    
                    else:
                        self.pd_queue.put(pd)
            except Queue.Empty:
                pass
                    
            try:    
                logging.debug("Scheduler Thread: " + str(self.__class__) + " Pilot Job")
                wu = self.wu_queue.get(True, 1)                
                if isinstance(wu, WorkUnit):                    
                    pj=self._schedule_wu(wu) 
                    if pj !=None:
                        wu = self.__expand_working_directory(wu, pj)                        
                        pj._submit_wu(wu)                    
                    else:
                        self.wu_queue.put(pd)
            except Queue.Empty:
                pass
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                logger.error("*** print_tb:")
                traceback.print_tb(exc_traceback, limit=1, file=sys.stderr)
                logger.error("*** print_exception:")
                traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stderr)
            time.sleep(5)        

        logging.debug("Re-Scheduler terminated")
    
    
    def __expand_working_directory(self, work_unit, pilot_job):
        """ Expand pilotdata:// url specified in the work_unit_description 
            to a local url on the machine of the PJ
            
            pilotdata://localhost/434bfc5c-23fd-11e1-a43f-00264a13ca4c
            
            to
            
           /tmp/pilotstore//434bfc5c-23fd-11e1-a43f-00264a13ca4c on machine running pilot_job        
        """ 
        working_directory=work_unit.work_unit_description["working_directory"]       
        if working_directory.find(PilotData.PD_ID_PREFIX)!=-1:
            pilot_data_url = working_directory
            pj_description = pilot_job.pilot_job_description
            pj_dc_affinity = pj_description["affinity_datacenter_label"]
            pj_machine_affinity = pj_description["affinity_machine_label"]
            ps = [s for i in self.pilot_store_services for s in i.list_pilotstores()]
            
            # find all pilot stores with the same affinity
            candidate_ps = []
            for i in ps:
                ps_description = i.pilot_store_description
                ps_dc_affinity = ps_description["affinity_datacenter_label"]
                ps_machine_affinity = ps_description["affinity_machine_label"]
                if ps_dc_affinity == pj_dc_affinity and ps_machine_affinity == pj_machine_affinity:
                    candidate_ps.append(i)
                
            # check whether required pilot_data is part of pilot_store
            target_ps = None  
            target_pd = None  
            for ps in candidate_ps:
                for pd in ps.list_pilotdata():
                    if pd.url == pilot_data_url:
                        logging.debug("Found PD %s at %s"%(pd.url, ps.service_url))
                        target_ps = ps 
                        target_pd = pd
                        break
            if target_pd == None:
                self.__stage_pd_to_pj(pilot_data_url, pj)
                
            ps_url = target_ps.url_for_pd(target_pd)
            components = urlparse.urlparse(ps_url)
            work_unit.work_unit_description["working_directory"] = components.path
            work_unit._update_work_unit_description(work_unit.work_unit_description)
            logging.debug("__expand_working_directory %s: Set working directory to %s"%(pilot_data_url, work_unit.work_unit_description["working_directory"]))
            return work_unit
         
        return work_unit
            
            
    def __stage_pd_to_pj(self, pilotdata, pilotjob):
        """
            stage required files to machine of pilot job
        """
        pass
    
    def __find_ps_at_pj_resource(self, pilotjob):
        pass
   
    
    
class WorkUnit(WorkUnit):
    """ WorkUnit - Wrapper for BigJob subjob """
    WU_ID_PREFIX="wu-"  

    def __init__(self, work_unit_description, work_data_service):
        self.id = self.WU_ID_PREFIX + str(uuid.uuid1())
        self.url = work_data_service.url + "/" + self.id
        self.state = State.New       
        self.subjob = None # reference to BigJob Subjob 
        self.work_unit_description = work_unit_description # WU Description
        self.subjob_description = self.__translate_wu_sj_description(work_unit_description)
        logger.debug("Created WU: %s"%self.url)     
                
    def get_id(self):
        return self.id
    
    
    def get_state(self):
        if self.subjob != None:
            return self.subjob.get_state()
        return self.state


    def cancel(self):
        if self.subjob != None:
            return self.subjob.cancel()
        return None

    def _update_work_unit_description(self, work_unit_description):
        self.work_unit_description = work_unit_description # WU Description
        self.subjob_description = self.__translate_wu_sj_description(work_unit_description)
        
    # INTERNAL
    def __translate_wu_sj_description(self, work_unit_description):
        jd = saga.job.description()
        if work_unit_description.has_key("executable"): 
            jd.executable = work_unit_description["executable"]
        jd.spmd_variation = "single"
        if work_unit_description.has_key("arguments"): 
            jd.arguments = work_unit_description["arguments"]
        
        if work_unit_description.has_key("number_of_processes"):
            jd.number_of_processes=str(work_unit_description["number_of_processes"])
        else:
            jd.number_of_processes="1"
        
        if work_unit_description.has_key("working_directory"): 
            jd.working_directory = work_unit_description["working_directory"]
        if work_unit_description.has_key("output"): 
            jd.output =  work_unit_description["output"]
        if work_unit_description.has_key("stderr"): 
            jd.error = work_unit_description["stderr"]
        return jd
        
