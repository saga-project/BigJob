""" TROY PilotData 
""" 
import sys
import os
import logging
logging.basicConfig(level=logging.DEBUG)

import uuid
import random
import threading
import time
import pdb
import Queue


import saga
sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))

from pstar.api.data.api import PilotData, DataUnit, PilotDataService
from pstar.api.compute.api import State
from pstar.impl.pilotstore_manager import *
from pstar.scheduler.random_scheduler import Scheduler

from pstar.coordination.advert import AdvertCoordinationAdaptor as CoordinationAdaptor

    

class PilotData(PilotData):
    """ TROY PilotData. Holds a set of data units.
    
        State model:
            New: PD object created
            Pending: PD object is currently updated  
            Running: At least 1 replica of PD is persistent in a pilot store            
    """
    
    PD_ID_PREFIX="pd-"  

    def __init__(self, pilot_data_service=None, pilot_data_description=None, pd_url=None):
        """
            1.) create a new Pilot Data: pilot_data_service and pilot_data_description required
            2.) reconnect to an existing Pilot Data: pd_url required 
            
        """
        if pd_url==None:
            self.id = self.PD_ID_PREFIX + str(uuid.uuid1())
            self.pilot_data_description = pilot_data_description        
            self.pilot_stores=[]
            self.url = CoordinationAdaptor.add_pd(pilot_data_service.url, self)
            self.state = State.New
            self.data_units = DataUnit.create_data_unit_list(self, self.pilot_data_description["file_urls"]) 
            CoordinationAdaptor.update_pd(self)
        else:
            self.id = self.__get_pd_id(pd_url)
            self.url = pd_url            
            self.__restore_state()
    
    
    def __restore_state(self):
        pd_dict = CoordinationAdaptor.get_pd(self.url)
        self.pilot_data_description = pd_dict["pilot_data_description"]
        self.state = pd_dict["state"]
        data_unit_dict_list = pd_dict["data_units"]
        self.data_units = [DataUnit.create_data_unit_from_dict(i) for i in data_unit_dict_list]
        self.pilot_stores = [] 
        for i in pd_dict["pilot_stores"]:
            logger.debug("PS:"+str(i)) 
            ps = PilotStore(ps_url=str(i))
            self.pilot_stores.append(ps) 
            
            
    def cancel(self):
        """ Cancel the PD. """
        self.state = State.Done    
        CoordinationAdaptor.update_pd(self)
            
    def add_data_unit(self, data_unit):
        self.data_units.append(data_unit)    
        CoordinationAdaptor.update_pd(self)
        # TODO Update Pilot Stores
        
        
    def remove_data_unit(self, data_unit):
        self.data_units.remove(data_unit)
        CoordinationAdaptor.update_pd(self)
        # TODO Update Pilot Stores
        
        
    def list_data_units(self):        
        return self.data_units
        
    
    def get_state(self):        
        return self.state  
    
    
    def add_pilot_store(self, pilot_store):
        """ add PD to a certain pilot store 
            data will be moved into this store
        """
        if len(self.pilot_stores) > 0: # copy files from other pilot store
            self.pilot_stores[0].copy_pd(self, pilot_store)
        else: # copy files from original location
            pilot_store.put_pd(self)
        self.pilot_stores.append(pilot_store)
        CoordinationAdaptor.update_pd(self)  
        
    
    def get_pilot_stores(self):
        """ get a list of pilot stores that have a copy of this PD """
        return self.pilot_stores
    
    
    def export(self, target_url):
        """ simple implementation of export: 
                copies file from first pilot store to local machine
        """
        if len(self.pilot_stores) > 0:
            self.pilot_stores[0].export_pd(self, target_url)
        else:
            logger.error("No Pilot Store for PD found")
    
    
    def to_dict(self):
        du_dict = self.__dict__
        du_dict["id"]=self.id
        return du_dict        
    
    ###########################################################################
    # BigData Internal Methods
    def update_state(self, state):
        self.state=state
        CoordinationAdaptor.update_pd(self)

    
    def __get_pd_id(self, pd_url):
        try:
            start = pd_url.index(self.PD_ID_PREFIX)
            end = pd_url.find("/", start)
            if end==-1:
                end = pd_url.find("?", start)
            if end==-1:
                end = len(pd_url)-1
            return pd_url[start:end]
        except:
            logger.error("No valid PD URL")
        return None
    
    def __repr__(self):        
        return "PD: " + str(self.url) 
        + " \nData Units: " + str(self.data_units)
        + " \nPilot Stores: " + str(self.pilot_stores)
    

class DataUnit(DataUnit):
    """ TROY DataUnit """
    DU_ID_PREFIX="du-"  
   
    def __init__(self, pd=None, local_url=None):        
        if local_url!=None:
            self.id = self.DU_ID_PREFIX + str(uuid.uuid1())
            self.local_url = local_url        
            if pd != None:
                self.url = pd.url + os.path.basename(local_url)
        
        
    @classmethod    
    def __exists_file(cls, url):   
        """ return True if file at url exists. Otherwise False """
        file_url = saga.url(url)
        if file_url.host == "":
            if os.path.exists(str(file_url)):
                return True
            else:
                return False            
        elif file_url.host=="localhost":
            if os.path.exists(file_url.path):
                return True
            else:
                return False
        else:            
            return True
        
    def to_dict(self):
        du_dict = self.__dict__
        du_dict["id"]=self.id
        return du_dict
    
    
    def __repr__(self):
        return str(self.__dict__) 
        
         
    @classmethod
    def create_data_unit_list(cls, pd=None, urls=None):
        """ Creates a list of DUs from URL list
        """    
        du_list = []    
        for i in urls:            
            if cls.__exists_file(i):
                du = DataUnit(pd, i)
                du_list.append(du)
    
        return du_list
    
    @classmethod
    def create_data_unit_from_urls(cls, urls=None):
        """ Creates a list of DUs from URL list
        """    
        du_list = []    
        for i in urls:            
            if cls.__exists_file(i):
                du = DataUnit(pd, i)
                du_list.append(du)
    
        return du_list
    
    
    @classmethod
    def create_data_unit_from_dict(cls, du_dict):
        du = DataUnit()
        logger.debug("Restore DU: " + str(du_dict))
        for i in du_dict.keys():
            du.__setattr__(i, du_dict[i])
        return du
    
    

    
###############################################################################
    
if __name__ == "__main__":        
    
    # What files? Create Pilot Data Description
    base_dir = "/Users/luckow/workspace-saga/applications/pilot-store/test/data1"
    url_list = os.listdir(base_dir)
    absolute_url_list = []
    for i in url_list:
        if os.path.isdir(i)==False:
            absolute_url_list.append(os.path.join(base_dir, i))
    pilot_data_description = {"file_urls":absolute_url_list}
    
    # create pilot data service
    pilot_data_service = PilotDataService()
    #note: will be scheduled as soon as a suitable pilot store is available
    pd = pilot_data_service.submit_pilot_data(pilot_data_description) 
    
    # create pilot store service (factory for pilot stores (physical, distributed storage))
    pilot_store_service = PilotStoreService()
    ps = pilot_store_service.create_pilotstore({
                                'service_url': "ssh://localhost/tmp/pilotstore/",
                                'size':100                                
                                })
    
    # add resources to pilot data service    
    pilot_data_service.add(pilot_store_service) 
    
    logging.debug("Finished setup of PSS and PDS. Waiting for scheduling of PD")
    
    while pd.get_state() != State.Done:
        state = pd.get_state()
        print "PD URL: %s State: %s"%(pd, state)
        if state==State.Running:
            break
        time.sleep(2)  
    
    logging.debug("Terminate Pilot Data/Store Service")
    pilot_data_service.cancel()
    pilot_store_service.cancel()
    
    
    
    
    