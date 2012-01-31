""" PilotData 
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
from bigjob import logger

from pilot.api import PilotData, DataUnit, PilotDataService
from pilot.api import State
from pilot.filemanagement.ssh_adaptor import SSHFileAdaptor 
from pilot.filemanagement.webhdfs_adaptor import WebHDFSFileAdaptor 
from pilot.coordination.advert import AdvertCoordinationAdaptor as CoordinationAdaptor


class PilotData(PilotData):
    """ PilotData. 
    
        Reserves a space of physical storage on the resource specified in the pilot_data_description
    """   
    
    PS_ID_PREFIX="ps-"   

        
    def __init__(self, pilot_data_service=None, pilot_data_description=None, ps_url=None):    
        """ 
            Initialize PilotData at given service url:
            
            ssh://<hostname>
            gsissh://<hostname>
            
            Currently only ssh schemes are supported. In the future all 
            SAGA URL schemes/adaptors should be supported.        
        """ 
        self.id = None
        self.url = None
        self.pilot_data_description = None
        self.service_url=None
        self.size = None
        self.data_unit_description = None
        self.data_units={}
        
        if ps_url==None and pilot_data_service!=None:      # new ps          
            self.id = self.PS_ID_PREFIX+str(uuid.uuid1())
            self.pilot_data_description = pilot_data_description
            self.url = CoordinationAdaptor.add_ps(CoordinationAdaptor.get_base_url(bigdata.application_id)+"/"+pilot_data_service.id, self)
        elif ps_url != None:
            logger.warn("Reconnect to PilotData: %s"%ps_url)
            dictionary = CoordinationAdaptor.get_ps(ps_url)
            ps_dict = dictionary["pilot_data"]
            for i in ps_dict:
                self.__setattr__(i, ps_dict[i])
                        
        self.initialize_pilot_data()
        
            
    def initialize_pilot_data(self):
        if self.pilot_data_description!=None:
            self.service_url=self.pilot_data_description["service_url"]
            self.size = self.pilot_data_description["size"]
            
            # initialize file adaptor
            if self.service_url.startswith("ssh:"):
                logger.debug("Use SSH backend")
                self.__filemanager = SSHFileAdaptor(self.service_url)
            elif self.service_url.startswith("http:"):
                logger.debug("Use WebHDFS backend")
                self.__filemanager = WebHDFSFileAdaptor(self.service_url)
                
            self.__filemanager.initialize_pilotdata()
            self.__filemanager.get_pilotdata_size()
            

    def __get_ps_id(self, ps_url):
        start = ps_url.index(self.PS_ID_PREFIX)
        end =ps_url.index("/", start)
        return ps_url[start:end]
    

    def cancel(self):        
        """ Cancel PilotData 

            Keyword arguments:
            None
        """
        self.__filemanager.delete_pilotdata()
        
        
    def url_for_du(self, du):
        if self.pilot_data.has_key(du.id):
            return self.service_url + "/" + str(du.id)
        return None
    
    
    def create_du(self, du):
        self.__filemanager.create_du(du.id)
        
        
    def put_du(self, du):
        logging.debug("Put PD: %s to PS: %s"%(du.id,self.service_url))
        self.__filemanager.create_du(du.id)
        self.__filemanager.put_du(du)
        self.pilot_data[du.id] = du
        CoordinationAdaptor.update_ps(self)
        
        
    def remove_du(self, du):
        """ Remove pilot data from pilot data """
        if self.pilot_data.has_key(du.id):
            self.__filemanager.remove_du(du)
            del self.pilot_data[du.id]
        CoordinationAdaptor.update_ps(self)
        
    
    def copy_du(self, du, ps_new):
        ps_new.create_du(du)
        self.__filemanager.copy_du(du, ps_new)
        
        # update meta data at ps_new
        ps_new.pilot_data[du.id] = du
        CoordinationAdaptor.update_ps(ps_new)
        
    
    def list_pilotdata(self):           
        return self.pilot_data.values()
    
    
    def get_state(self):
        return self.__filemanager.get_state()
    
    
    def export_du(self, du, target_url):
        self.__filemanager.get_du(du, target_url)
    
    
    def to_dict(self):
        ps_dict = {}
        ps_dict["id"]=self.id
        ps_dict["url"]=self.url
        ps_dict["pilot_data_description"]=self.pilot_data_description
        logger.debug("PS Dictionary: " + str(ps_dict))
        return ps_dict
    
    
    def __repr__(self):
        return self.service_url
    
    
    @classmethod
    def create_pilot_data_from_dict(cls, ps_dict):
        ps = PilotData()
        for i in ps_dict.keys():
            ps.__setattr__(i, ps_dict[i])
        ps.initialize_pilot_data()
        logger.debug("created ps " + str(ps))
        return ps
    

class PilotDataService(PilotDataService):
    """ PilotDataService (PSS)."""
    
    PSS_ID_PREFIX="pss-"

    # Class members
    __slots__ = (
        'id',             # Reference to this PJS
        'url',            # URL for referencing PilotDataService
        'state',          # Status of the PJS
        'data_unit'    # List of PJs under this PJS
        'affinity_list'   # List of PS on that are affine to each other
    )

    def __init__(self, pss_url=None):
        """ Create a PilotDataService

            Keyword arguments:
            pss_id -- restore from pss_id
        """        
        self.pilot_data={}
        
        if pss_url == None:
            self.id = self.PSS_ID_PREFIX + str(uuid.uuid1())
            application_url = CoordinationAdaptor.get_base_url(bigdata.application_id)
            self.url = CoordinationAdaptor.add_pss(application_url, self)
        else:
            self.id = self.__get_pss_id(pss_url)
    
    
    def __get_pss_id(self, pss_url):
        start = pss_url.index(self.PSS_ID_PREFIX)
        end =pss_url.index("/", start)
        return pss_url[start:end]
    
    def __restore_ps(self, pss_url):
        ps_list=CoordinationAdaptor.list_ps(pss_url) 
        for i in ps_list:
           pass

    def create_pilot(self, pilot_data_description):
        """ Create a PilotData 

            Keyword arguments:
            pilot_data_description -- PilotData Description    
            {
                'service_url': "ssh://<hostname>/base-url/"                
                'size': "1000"
            }
            Return value:
            A PilotData handle
        """
        ps = PilotData(pilot_data_service=self, 
                        pilot_data_description=pilot_data_description)
        self.pilot_data[ps.id]=ps
        
        # store pilot store in central data space
        CoordinationAdaptor.add_ps(self.url, ps)        
        return ps
    
    
    def get_pilot(self, ps_id):
        if self.pilot_data.has_key(ps_id):
            return self.pilot_data[ps_id]
        return None


    def list_pilots(self):
        """ List all PSs of PSS """
        return self.pilot_data.values()
    

    def cancel(self):
        """ Cancel the PilotDataService.
            
            Keyword arguments:
            None

            Return value:
            Result of operation
        """
        for i in self.pilot_data.values():
            i.cancel()
 
    
    def to_dict(self):
        pss_dict = self.__dict__
        pss_dict["id"]=self.id
        return pss_dict
 
 
    def __del__(self):
        self.cancel()         
            
    

class DataUnit(DataUnit):
    """ Holds a set of file
    
        State model:
            New: PD object created
            Pending: PD object is currently updated  
            Running: At least 1 replica of PD is persistent in a pilot store            
    """
    
    DU_ID_PREFIX="du-"  

    def __init__(self, pilot_data_service=None, data_unit_description=None, pd_url=None):
        """
            1.) create a new Pilot Data: pilot_data_service and data_unit_description required
            2.) reconnect to an existing Pilot Data: pd_url required 
            
        """
        if pd_url==None:
            self.id = self.DU_ID_PREFIX + str(uuid.uuid1())
            self.data_unit_description = data_unit_description        
            self.pilot_data=[]
            self.url = CoordinationAdaptor.add_pd(pilot_data_service.url, self)
            self.state = State.New
            self.data_unit_items = DataUnitItem.create_data_unit_list(self, self.data_unit_description["file_urls"]) 
            CoordinationAdaptor.update_pd(self)
        else:
            self.id = self.__get_pd_id(pd_url)
            self.url = pd_url            
            self.__restore_state()
    
    
    def __restore_state(self):
        pd_dict = CoordinationAdaptor.get_pd(self.url)
        self.data_unit_description = pd_dict["data_unit_description"]
        self.state = pd_dict["state"]
        data_unit_dict_list = pd_dict["data_units"]
        self.data_unit_items = [DataUnitItem.create_data_unit_from_dict(i) for i in data_unit_dict_list]
        self.pilot_stores = [] 
        for i in pd_dict["pilot_stores"]:
            logger.debug("PS:"+str(i)) 
            ps = PilotData(ps_url=str(i))
            self.pilot_stores.append(ps) 
            
            
    def cancel(self):
        """ Cancel the PD. """
        self.state = State.Done    
        CoordinationAdaptor.update_pd(self)
            
    def add_data_unit(self, data_unit):
        self.data_unit_items.append(data_unit)    
        CoordinationAdaptor.update_pd(self)
        # TODO Update Pilot Stores
        
        
    def remove_data_unit(self, data_unit):
        self.data_unit_items.remove(data_unit)
        CoordinationAdaptor.update_pd(self)
        # TODO Update Pilot Stores
        
        
    def list_data_units(self):        
        return self.data_unit_items
        
    
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
        + " \nData Units: " + str(self.data_unit_items)
        + " \nPilot Stores: " + str(self.pilot_stores)
    

class DataUnitItem():
    """ DataUnitItem """
    DUI_ID_PREFIX="dui-"  
   
    def __init__(self, pd=None, local_url=None):        
        if local_url!=None:
            self.id = self.DUI_ID_PREFIX + str(uuid.uuid1())
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
    
    