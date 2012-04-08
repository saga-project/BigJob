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
import pilot
from bigjob import logger
from pilot.api import ComputeDataService, ComputeUnit, State
from pilot.impl.pilotdata_manager import PilotData, DataUnit
#from pilot.coordination.advert import AdvertCoordinationAdaptor as CoordinationAdaptor
from pilot.coordination.nocoord import NoCoordinationAdaptor as CoordinationAdaptor
""" Loaded Module determines scheduler:
    
    bigdata.scheduler.data_compute_scheduler - selects random locations for PD and WUs
    bigdata.scheduler.data_compute_affinity_scheduler - considers affinity descriptions
    
"""

from pilot.scheduler.data_compute_affinity_scheduler import Scheduler

class ComputeDataService(ComputeDataService):
    """ TROY WorkDataService.
    
        The WorkDataService is the application's interface to submit 
        ComputeUnits and PilotData/DataUnit to the Pilot-Manager 
        in the P* Model.
    """    
    CDS_ID_PREFIX="cds-"  


    def __init__(self, cds_url=None):
        """ Create a Work Data Service object.

            Keyword arguments:
            cds_url -- Reconnect to an existing WDS (optional).
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
        self.scheduler_thread.start()

    def __get_cds_id(self, cds_url):
        start = cds_url.index(self.CDS_ID_PREFIX)
        end =cds_url.index("/", start)
        return cds_url[start:end]


    ###########################################################################
    # Pilot Job
    
    def add_pilot_compute_service(self, pjs):
        """ Add a PilotJobService to this CDS.

            Keyword arguments:
            pilotjob_services -- The PilotJob Service(s) to which this 
                                 Work Unit Service will connect.

            Return:
            Result
        """
        self.pilot_job_services.append(pjs)
        CoordinationAdaptor.update_cds(self.url, self)

    def remove_pilot_compute_service(self, pjs):
        """ Remove a PilotJobService from this CDS.

            Note that it won't cancel the PilotJobService, it will just no
            longer be connected to this WUS.

            Keyword arguments:
            pilotjob_services -- The PilotJob Service(s) to remove from this
                                 Work Unit Service. 

            Return:
            Result
        """
        self.pilot_job_services.remove(pjs)
        CoordinationAdaptor.update_cds(self.url, self)

    def submit_compute_unit(self, compute_unit_description):
        """ Submit a WU to this Work Unit Service.

            Keyword argument:
            cud -- The ComputeUnitDescription from the application

            Return:
            ComputeUnit object
        """
        cu = ComputeUnit(compute_unit_description, self)
        self.compute_units[cu.id]=cu
        self.cu_queue.put(cu)
        CoordinationAdaptor.update_cds(self.url, self)
        return cu
    
    
    ###########################################################################
    # Pilot Data     
    def add_pilot_data_service(self, pds):
        """ Add a PilotDataService 

            Keyword arguments:
            pds -- The PilotDataService to add.

            Return:
            None
        """
        self.pilot_data_services.append(pds)
        CoordinationAdaptor.update_cds(self.url, self)
    
    def remove_pilot_data_service(self, pds):

        """ Remove a PilotDataService 
            
            Keyword arguments:
            pds -- The PilotDataService to remove 
            
            Return:
            None
        """
        self.pilot_data_services.remove(pds)
        CoordinationAdaptor.update_cds(self.url, self)
    
    
    def list_pilot_compute(self):
        """ List all pilot compute of CDS """
        return self.pilot_job_service
    
    
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
        du = DataUnit(pilot_data_service=self, 
                      data_unit_description=data_unit_description)
        self.data_units[du.id]=du
        self.du_queue.put(du)
        # queue currently not persisted
        CoordinationAdaptor.update_cds(self.url, self)
        return du
    
    
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
        CoordinationAdaptor.delete_cds(self.url)
   
    def wait(self):
        """ Waits for CUs and DUs
            Return if all du's are running 
                   AND
                   cu's are done
            
        """
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

        #[pc.wait() for i in self.pilot_job_services for pc in i.list_pilots()]
        #[pd.wait() for i in self.pilot_data_services for pd in i.list_pilots()]
                
        
    def get_state(self):
        return self.state
    
    
    def get_id(self):
        return str(self.id)
   
    ###########################################################################
    # Internal Scheduling
    def __update_scheduler_resources(self):
        logging.debug("__update_scheduler_resources")        
        pd = [s for i in self.pilot_data_services for s in i.list_pilots()]
        self.scheduler.set_pilot_data(pd)
        pj = [p for i in self.pilot_job_services for p in i.list_pilots()]
        logging.debug("Pilot-Jobs: " + str(pj))
        self.scheduler.set_pilot_jobs(pj)
    
    def _schedule_du(self, du):
        """ Schedule DU to a suitable pilot data
        
            Currently one level of scheduling is used:
                1.) Add all resources managed by the contained PDS 
                2.) Select one resource
        """ 
        logging.debug("Schedule PD")
        self.__update_scheduler_resources()
        selected_pilot_data = self.scheduler.schedule_pilot_data(du.data_unit_description)
        return selected_pilot_data 
    
    def _schedule_cu(self, cu):
        logging.debug("Schedule CU")
        self.__update_scheduler_resources()
        selected_pilot_job = self.scheduler.schedule_pilot_job(cu.compute_unit_description)
        return selected_pilot_job
    
    def _scheduler_thread(self):
        while True and self.stop.isSet()==False:            
            try:
                logging.debug("Scheduler Thread: " + str(self.__class__) + " Pilot Data")
                du = self.du_queue.get(True, 1)  
                # check whether this is a real du object  
                if isinstance(du, DataUnit):
                    pd=self._schedule_du(du)                
                    if(pd!=None):                        
                        logging.debug("Initiate Transfer to PD.")
                        du.add_pilot_data(pd)
                        logging.debug("Transfer to PD finished.")
                        du.update_state(State.Running) 
                        self.du_queue.task_done()                   
                    else:
                        self.du_queue.task_done() 
                        self.du_queue.put(du)
            except Queue.Empty:
                pass
                    
            try:    
                logging.debug("Scheduler Thread: " + str(self.__class__) + " Pilot Job")
                cu = self.cu_queue.get(True, 1)                
                if isinstance(cu, ComputeUnit):                    
                    pj=self._schedule_cu(cu) 
                    if pj !=None:
                        cu = self.__expand_working_directory(cu, pj)                        
                        pj._submit_cu(cu)           
                        self.cu_queue.task_done()         
                    else:
                        self.cu_queue.task_done() 
                        self.cu_queue.put(du)
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
    
    
    def __expand_working_directory(self, compute_unit, pilot_job):
        """ Expand pilotdata:// url specified in the compute_unit_description 
            to a local url on the machine of the PJ
            
            pilotdata://localhost/434bfc5c-23fd-11e1-a43f-00264a13ca4c
            
            to
            
           /tmp/pilotstore//434bfc5c-23fd-11e1-a43f-00264a13ca4c on machine running pilot_job        
        """ 
        if compute_unit.compute_unit_description.has_key("working_directory"):
            working_directory=compute_unit.compute_unit_description["working_directory"]       
            if working_directory.find(DataUnit.DU_ID_PREFIX)!=-1:
                pilot_data_url = working_directory
                pj_description = pilot_job.pilot_compute_description
                pj_dc_affinity = pj_description["affinity_datacenter_label"]
                pj_machine_affinity = pj_description["affinity_machine_label"]
                pd = [s for i in self.pilot_data_services for s in i.list_pilots()]
                
                # find all pilot stores with the same affinity
                candidate_pd = []
                for i in pd:
                    pd_description = i.pilot_data_description
                    pd_dc_affinity = pd_description["affinity_datacenter_label"]
                    pd_machine_affinity = pd_description["affinity_machine_label"]
                    if pd_dc_affinity == pj_dc_affinity and pd_machine_affinity == pj_machine_affinity:
                        candidate_pd.append(i)
                    
                # check whether required pilot_data is part of pilot_data
                target_pd = None  
                target_du = None  
                for pd in candidate_pd:
                    for du in pd.list_data_units():
                        logger.debug("DU URL:%s"%(du.url))
                        if du.url == pilot_data_url:
                            logging.debug("Found PD %s at %s"%(du.url, pd.service_url))
                            target_pd = pd 
                            target_du = du
                            break
                if target_du == None:
                    self.__stage_du_to_pj(pilot_data_url, pilot_job)
                
                if target_pd!=None:
                    pd_url = target_pd.url_for_du(target_du)
                    components = urlparse.urlparse(pd_url)
                    compute_unit.compute_unit_description["working_directory"] = components.path
                    compute_unit._update_compute_unit_description(compute_unit.compute_unit_description)
                    logging.debug("__expand_working_directory %s: Set working directory to %s"%(pilot_data_url, compute_unit.compute_unit_description["working_directory"]))
                    return compute_unit
         
        return compute_unit
            
            
    def __stage_du_to_pj(self, pilotdata, pilotjob):
        """
            stage required files to machine of pilot job
        """
        pass
    
    def __find_pd_at_pj_resource(self, pilotjob):
        pass
   
    
    
class ComputeUnit(ComputeUnit):
    """ ComputeUnit - Wrapper for BigJob subjob """
    CU_ID_PREFIX="cu-"  

    def __init__(self, compute_unit_description, work_data_service):
        self.id = self.CU_ID_PREFIX + str(uuid.uuid1())
        self.url = work_data_service.url + "/" + self.id
        self.state = State.New       
        self.subjob = None # reference to BigJob Subjob 
        self.compute_unit_description = compute_unit_description # WU Description
        self.subjob_description = self.__translate_cu_sj_description(compute_unit_description)
        logger.debug("Created CU: %s"%self.url)     
                
    def get_id(self):
        return self.id
    
    
    def get_state(self):
        if self.subjob != None:
            return self.subjob.get_state()
        return self.state
    
    
    def wait(self):
        """ Wait until in Done state 
            (or Failed state)
        """
        while True:
            state = self.get_state()
            logger.debug("Compute Unit - State: %s"%state)            
            if state==State.Done or state==State.Failed:
                break
            time.sleep(2)
            
        #logger.debug("### END CU wait")

    
    def cancel(self):
        if self.subjob != None:
            return self.subjob.cancel()
        return None

    
    def _update_compute_unit_description(self, compute_unit_description):
        self.compute_unit_description = compute_unit_description # WU Description
        self.subjob_description = self.__translate_cu_sj_description(compute_unit_description)
        
    # INTERNAL
    def __translate_cu_sj_description(self, compute_unit_description):
        jd = saga.job.description()
        if compute_unit_description.has_key("executable"): 
            jd.executable = compute_unit_description["executable"]
        jd.spmd_variation = "single"
        if compute_unit_description.has_key("arguments"): 
            jd.arguments = compute_unit_description["arguments"]
        
        if compute_unit_description.has_key("number_of_processes"):
            jd.number_of_processes=str(compute_unit_description["number_of_processes"])
        else:
            jd.number_of_processes="1"
        
        if compute_unit_description.has_key("working_directory"): 
            jd.working_directory = compute_unit_description["working_directory"]
        if compute_unit_description.has_key("output"): 
            jd.output =  compute_unit_description["output"]
        if compute_unit_description.has_key("error"): 
            jd.error = compute_unit_description["error"]
        return jd
        
