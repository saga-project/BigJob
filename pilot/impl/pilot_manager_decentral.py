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

class ComputeDataServiceDecentral(pilot.api.ComputeDataService):
    """ ComputeDataServiceDecentral.
    
        The ComputeDataService is the application's interface to submit 
        ComputeUnits and PilotData/DataUnit to the Pilot-Manager 
        in the P* Model.
        
        The decentral ComputeDateService will only work with Redis!
    """    
    CDS_ID_PREFIX="cds-"  


    def __init__(self, cds_url=None):
        """ Create a ComputeDataService object.

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
    
    
    ###########################################################################
    # Compute Data Service private methods
    
    def __submit_cu(self, compute_unit):
        """ Submits compute unit to Bigjob """
                
        if len(self.pilot_job_services)!=1:
            raise PilotError("No PilotComputeService found. Please start a PCS before submitting ComputeUnits.")
        
        sj = subjob()
        self.pcs_coordination_namespace=self.pilot_job_services[0].coordination_queue
        logger.debug("Submit CU to big-job via external queue: %s"%self.pcs_coordination_namespace)
        sj.submit_job(self.pcs_coordination_namespace, compute_unit.subjob_description)
        compute_unit._update_subjob(sj)
        return compute_unit
    
    
    ###########################################################################
    # Pilot Data     
    def add_pilot_data_service(self, pds):
        """ Not implemented yet"""
        raise NotImplementedError("Not implemented")
    
    
    def remove_pilot_data_service(self, pds):
        """ Not implemented yet"""        
        raise NotImplementedError("Not implemented")
        
    
    def list_pilot_compute(self):
        """ List all pilot compute of CDS """
        return self.pilot_job_service
    
    
    def list_pilot_data(self):
        """ Not implemented yet""" 
        raise NotImplementedError("Not implemented")
    
    
    def list_data_units(self):
        """ Not implemented yet"""
        raise NotImplementedError("Not implemented")
    
    
    def get_data_unit(self, du_id):
        """ Not implemented yet"""
        raise NotImplementedError("Not implemented")
    
    
    def submit_data_unit(self, data_unit_description):
        """ Not implemented yet"""
        raise NotImplementedError("Not implemented")
    
    
    def cancel(self):
        """ Cancel the CDS. 
            All associated PD and PC objects are canceled.            
        """
        CoordinationAdaptor.delete_cds(self.url)
   
   
   
    def wait(self):
        """ Waits for CUs and DUs. Return after all DU's have been placed (i.e. in state Running)
            and all CU's have been completed (i.e. in state Done) or if a fault has occurred or
            the user has cancelled a CU or DU.            
        """
        try:
            logger.debug("### START WAIT ###")
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
        return self.state
    
    
    def get_id(self):
        return str(self.id)
    
    
    def __del__(self):
        """ Make sure that background thread terminates"""
        self.cancel()
   
    
   
    
   