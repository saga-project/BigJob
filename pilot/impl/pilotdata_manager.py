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
try:
    from pilot.filemanagement.webhdfs_adaptor import WebHDFSFileAdaptor
except:
    logger.warn("WebHDFS package not found.") 
try:
    from pilot.filemanagement.globusonline_adaptor import GlobusOnlineFileAdaptor
except:
    logger.warn("Globus Online package not found.") 



#from pilot.coordination.advert import AdvertCoordinationAdaptor as CoordinationAdaptor
from pilot.coordination.nocoord import NoCoordinationAdaptor as CoordinationAdaptor


# generate global application id for this instance
#application_id = str(uuid.uuid1())
application_id = "bigdata"

class PilotData(PilotData):
    """ PilotData. 
    
        Reserves a space of physical storage on the resource specified in the pilot_data_description
    """   
    
    PD_ID_PREFIX="pd-"   

        
    def __init__(self, pilot_data_service=None, pilot_data_description=None, pd_url=None):    
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
        
        if pd_url==None and pilot_data_service!=None:      # new pd          
            self.id = self.PD_ID_PREFIX+str(uuid.uuid1())
            self.pilot_data_description = pilot_data_description
            self.url = CoordinationAdaptor.add_pd(CoordinationAdaptor.get_base_url(application_id)+"/"+pilot_data_service.id, self)
        elif pd_url != None:
            logger.warn("Reconnect to PilotData: %s"%pd_url)
            dictionary = CoordinationAdaptor.get_pd(pd_url)
            pd_dict = dictionary["pilot_data"]
            for i in pd_dict:
                self.__setattr__(i, pd_dict[i])
                        
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
            

    def __get_pd_id(self, pd_url):
        start = pd_url.index(self.PD_ID_PREFIX)
        end =pd_url.index("/", start)
        return pd_url[start:end]
    

    def cancel(self):        
        """ Cancel PilotData 

            Keyword arguments:
            None
        """
        #self.__filemanager.delete_pilotdata()
        pass
        
        
    def url_for_du(self, du):
        if self.data_units.has_key(du.id):
            return self.service_url + "/" + str(du.id)
        return None
    
    
    def create_du(self, du):
        self.__filemanager.create_du(du.id)
        
        
    def put_du(self, du):
        logging.debug("Put PD: %s to PS: %s"%(du.id,self.service_url))
        self.__filemanager.create_du(du.id)
        self.__filemanager.put_du(du)
        self.data_units[du.id] = du
        CoordinationAdaptor.update_pd(self)
        
        
    def remove_du(self, du):
        """ Remove pilot data from pilot data """
        if self.data_units.has_key(du.id):
            self.__filemanager.remove_du(du)
            del self.data_units[du.id]
        CoordinationAdaptor.update_pd(self)
        
    
    def copy_du(self, du, pd_new):
        pd_new.create_du(du)
        self.__filemanager.copy_du(du, pd_new)
        
        # update meta data at pd_new
        pd_new.data_units[du.id] = du
        CoordinationAdaptor.update_pd(pd_new)
        
    
    def list_data_units(self):
        return self.data_units.values()           
        #return self.data_units.values()
    
    
    def get_state(self):
        return self.__filemanager.get_state()
    
    
    def wait(self):
        while 1:
            finish_counter=0
            result_map = {}
            dus = self.data_units.values()
            for du in dus: 
                du.wait()
                state = du.get_state()           
                #state = job_detail["state"]                
                if result_map.has_key(state)==False:
                    result_map[state]=1
                else:
                    result_map[state] = result_map[state]+1
                if self.__has_finished(state)==True:
                    finish_counter = finish_counter + 1                   
            logger.debug("PD ID: " + str(self.id) + " Total DUs: %s States: %s"%(len(dus), str(result_map)))
            if finish_counter == len(dus):
                break
            time.sleep(2)

    
    def export_du(self, du, target_url):
        self.__filemanager.get_du(du, target_url)
    
    
    def to_dict(self):
        pd_dict = {}
        pd_dict["id"]=self.id
        pd_dict["url"]=self.url
        pd_dict["pilot_data_description"]=self.pilot_data_description
        logger.debug("PS Dictionary: " + str(pd_dict))
        return pd_dict
    
    
    def __repr__(self):
        return self.service_url
    
    
    def __has_finished(self, state):
        state = state.lower()
        if state=="running" or state=="failed" or state=="canceled":
            return True
        else:
            return False
    
    @classmethod
    def create_pilot_data_from_dict(cls, pd_dict):
        pd = PilotData()
        for i in pd_dict.keys():
            pd.__setattr__(i, pd_dict[i])
        pd.initialize_pilot_data()
        logger.debug("created pd " + str(pd))
        return pd
    

class PilotDataService(PilotDataService):
    """ PilotDataService (PSS)."""
    
    PDS_ID_PREFIX="pds-"

    # Class members
    __slots__ = (
        'id',             # Reference to this PJS
        'url',            # URL for referencing PilotDataService
        'state',          # Status of the PJS
        'data_unit'    # List of PJs under this PJS
        'affinity_list'   # List of PS on that are affine to each other
    )

    def __init__(self, pds_url=None):
        """ Create a PilotDataService

            Keyword arguments:
            pds_id -- restore from pds_id
        """        
        self.pilot_data={}
        
        if pds_url == None:
            self.id = self.PDS_ID_PREFIX + str(uuid.uuid1())
            application_url = CoordinationAdaptor.get_base_url(application_id)
            self.url = CoordinationAdaptor.add_pds(application_url, self)
        else:
            self.id = self.__get_pds_id(pds_url)
    
    
    def __get_pds_id(self, pds_url):
        start = pds_url.index(self.PDS_ID_PREFIX)
        end =pds_url.index("/", start)
        return pds_url[start:end]
    
    def __restore_pd(self, pds_url):
        pd_list=CoordinationAdaptor.list_pd(pds_url) 
        for i in pd_list:
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
        pd = PilotData(pilot_data_service=self, 
                        pilot_data_description=pilot_data_description)
        self.pilot_data[pd.id]=pd
        
        # store pilot data in central data space
        CoordinationAdaptor.add_pd(self.url, pd)        
        return pd
    
    
    def get_pilot(self, pd_id):
        if self.pilot_data.has_key(pd_id):
            return self.pilot_data[pd_id]
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
 
 
    def wait(self):
        for i in self.pilot_data.values():
            i.wait()
 
    
    def to_dict(self):
        pds_dict = self.__dict__
        pds_dict["id"]=self.id
        return pds_dict
 
 
    def __del__(self):
        self.cancel()         
            
    

class DataUnit(DataUnit):
    """ Holds a set of file
    
        State model:
            New: PD object created
            Pending: PD object is currently updated  
            Running: At least 1 replica of PD is persistent in a pilot data            
    """
    
    DU_ID_PREFIX="du-"  

    def __init__(self, pilot_data_service=None, data_unit_description=None, du_url=None):
        """
            1.) create a new Pilot Data: pilot_data_service and data_unit_description required
            2.) reconnect to an existing Pilot Data: du_url required 
            
        """
        if du_url==None:
            self.id = self.DU_ID_PREFIX + str(uuid.uuid1())
            self.data_unit_description = data_unit_description        
            self.pilot_data=[]
            self.url = CoordinationAdaptor.add_du(pilot_data_service.url, self)
            self.state = State.New
            self.data_unit_items = DataUnitItem.create_data_unit_list(self, self.data_unit_description["file_urls"]) 
            CoordinationAdaptor.update_du(self)
        else:
            self.id = self.__get_du_id(du_url)
            self.url = du_url   
            logger.debug("Restore du: %s"%self.id)         
            self.__restore_state()
            
        self.transfer_threads=[]
    
    
    def __restore_state(self):
        du_dict = CoordinationAdaptor.get_du(self.url)
        self.data_unit_description = du_dict["data_unit_description"]
        self.state = du_dict["state"]
        data_unit_dict_list = du_dict["data_units"]
        self.data_unit_items = [DataUnitItem.create_data_unit_from_dict(i) for i in data_unit_dict_list]
        
        # restore pilot data
        self.pilot_data = [] 
        for i in du_dict["pilot_data"]:
            logger.debug("PD: "+str(i)) 
            pd = PilotData(pd_url=str(i))
            self.pilot_data.append(pd) 
            
            
    def cancel(self):
        """ Cancel the DU. """
        self.state = State.Done    
        CoordinationAdaptor.update_du(self)
            
    def add_data_unit(self, data_unit):
        self.data_unit_items.append(data_unit)    
        CoordinationAdaptor.update_du(self)
        # TODO Update Pilot Data
        
        
    def remove_data_unit(self, data_unit):
        self.data_unit_items.remove(data_unit)
        CoordinationAdaptor.update_du(self)
        # TODO Update Pilot Data
        
        
    def list_data_unit_items(self):        
        return self.data_unit_items
        
    
    def get_state(self):        
        return self.state  
    
    
    def wait(self):
        """ Wait until in running state 
            (or failed state)
        """
        logger.debug("DU: %s wait()"%(str(self.id)))
        # Wait for all transfers to finish
        for i in self.transfer_threads:
            i.join()
        
        # Wait for state to change
        while self.state!=State.Running and self.state!=State.Failed:
            logger.debug("State: %s"%self.state)
            time.sleep(2)
    
    
    def add_pilot_data(self, pilot_data):
        """ add DU to a certain pilot data 
            data will be moved into this data
        """
        transfer_thread=threading.Thread(target=self.__add_pilot_data, args=[pilot_data])
        transfer_thread.start()        
        self.transfer_threads.append(transfer_thread)
        
    
    def get_pilot_data(self):
        """ get a list of pilot data that have a copy of this PD """
        return self.pilot_data
    
    
    def export(self, target_url):
        """ simple implementation of export: 
                copies file from first pilot data to local machine
        """
        if len(self.pilot_data) > 0:
            self.pilot_data[0].export_du(self, target_url)
        else:
            logger.error("No Pilot Data for PD found")
    
    
    def to_dict(self):
        du_dict = self.__dict__
        du_dict["id"]=self.id
        return du_dict        
    
    
    ###########################################################################
    # BigData Internal Methods
    def update_state(self, state):
        self.state=state
        CoordinationAdaptor.update_du(self)

    
    def __add_pilot_data(self, pilot_data):
        logger.debug("add du to pilot data")
        if len(self.pilot_data) > 0: # copy files from other pilot data
            self.pilot_data[0].copy_du(self, pilot_data)
        else: # copy files from original location
            pilot_data.put_du(self)
        self.pilot_data.append(pilot_data)
        CoordinationAdaptor.update_du(self)  
        
        
    def __get_du_id(self, du_url):
        try:
            start = du_url.index(self.DU_ID_PREFIX)
            end = du_url.find("/", start)
            if end==-1:
                end = du_url.find("?", start)
            if end==-1:
                end = len(du_url)-1
            return du_url[start:end]
        except:
            logger.error("No valid PD URL")
        return None
    
    
    def __repr__(self):        
        return "PD: " + str(self.url) 
        + " \nData Units: " + str(self.data_unit_items)
        + " \nPilot Stores: " + str(self.pilot_data)
    
    

class DataUnitItem(object):
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
                du = DataUnitItem(pd, i)
                du_list.append(du)
    
        return du_list
    
    @classmethod
    def create_data_unit_from_urls(cls, pd=None, urls=None):
        """ Creates a list of DUs from URL list
        """    
        du_list = []    
        for i in urls:            
            if cls.__exists_file(i):
                du = DataUnitItem(pd, i)
                du_list.append(du)
    
        return du_list
    
    
    @classmethod
    def create_data_unit_from_dict(cls, du_dict):
        du = DataUnitItem()
        logger.debug("Restore DU: " + str(du_dict))
        for i in du_dict.keys():
            logger.debug("Set attribute: %s", i)
            du.__setattr__(i, du_dict[i])
        return du
    
    
