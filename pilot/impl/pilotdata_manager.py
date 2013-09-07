
""" B{PilotData Module}: Implementation of L{PilotData}, L{PilotDataService} and L{DataUnit}
""" 
import sys
import os
import logging
import uuid
import random
import threading
import time
import pdb
import Queue
import socket
import tldextract
tldextract.tldextract.LOG.setLevel(logging.WARNING)
import difflib


from pilot.api.api import PilotError

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from bigjob import logger
from pilot.api import PilotData, DataUnit, PilotDataService, State


""" Load file management adaptors """
from pilot.filemanagement.ssh_adaptor import SSHFileAdaptor 
try:
    from pilot.filemanagement.webhdfs_adaptor import WebHDFSFileAdaptor
except:
    logger.warn("WebHDFS package not found.") 
try:
    from pilot.filemanagement.globusonline_adaptor import GlobusOnlineFileAdaptor
except:
    logger.warn("Globus Online package not found.") 
    
try:
    from pilot.filemanagement.gs_adaptor import GSFileAdaptor
except:
    logger.warn("Goggle Storage package not found.") 


try:
    from pilot.filemanagement.s3_adaptor import S3FileAdaptor
except:
    logger.warn("Amazon S3 package not found.") 

try:
    from pilot.filemanagement.irods_adaptor import iRodsFileAdaptor
except:
    logger.warn("iRods Storage package not found.") 



#from pilot.coordination.advert import AdvertCoordinationAdaptor as CoordinationAdaptor
#from pilot.coordination.nocoord import NoCoordinationAdaptor as CoordinationAdaptor
from pilot.coordination.redis_adaptor import RedisCoordinationAdaptor as CoordinationAdaptor
from saga import Url as SAGAUrl


# generate global application id for this instance
application_id = "bigdata"

"""
    Implementation considerations:
    
    PilotDataService can have n PilotData 
    PilotData can have n DataUnits (only DU URLs is held to avoid circular dependencies)
    A DataUnit can be part of n PilotData 
"""

class PilotData(PilotData):
    """ B{PilotData (PD).} 
                
        This is the object that is returned by the PilotDataService when a 
        new PilotData is created based on a PilotDataDescription. A PilotData represents
        a finite amount of physical space on a certain resource. It can be populated
        with L{DataUnit}s.

        The PilotData object can be used by the application to keep track 
        of a pilot. A PilotData has state, can be queried, can be cancelled.
        
    """   
    
    PD_ID_PREFIX="pd-"   

        
    def __init__(self, pilot_data_service=None, pilot_data_description=None, pd_url=None):    
        """ 
            Initialize PilotData at given service url::
            
                ssh://<hostname>
                gsissh://<hostname>
                go://<hostname>
                gs://google.com
                s3://aws.amazon.com
            
            In the future more SAGA/Bliss URL schemes/adaptors are supported.        
        """ 
        self.id = None
        self.url = pd_url
        self.pilot_data_description = None
        self.pilot_data_service = pilot_data_service
        self.service_url=None
        self.size = None
        self.data_unit_urls = []
        self.security_context = None
        
        if pd_url==None and pilot_data_service!=None:      # new pd          
            self.id = self.PD_ID_PREFIX+str(uuid.uuid1())
            self.pilot_data_description = pilot_data_description
            self.url = CoordinationAdaptor.add_pd(CoordinationAdaptor.get_base_url(application_id)+":"+pilot_data_service.id, self)
        elif pd_url != None:
            logger.warn("Reconnect to PilotData: %s"%pd_url)
            dictionary = CoordinationAdaptor.get_pd(pd_url)
            if dictionary.has_key("security_context"):
                self.security_context=dictionary["security_context"]
            pd_dict = eval(dictionary["pilot_data"])
            for i in pd_dict:
                self.__setattr__(i, pd_dict[i])
            # A Pilot Data does not hold a direct reference to a Data Unit (only URL refs are stored)
            self.data_unit_urls = eval(dictionary["data_unit_urls"])
                        
        self.__initialize_pilot_data()
        CoordinationAdaptor.update_pd(self)
    

    def cancel(self):        
        """ Cancel PilotData  """
        #self.__filemanager.delete_pilotdata()
        pass
    
     
    def get_url(self):
        """ Get URL of PilotData. Used for reconnecting to PilotData """
        return self.url
       
    
    def url_for_du(self, du):
        """ Get full URL to DataUnit within PilotData """
        return self.service_url + "/" + str(du.id)
        

    def submit_data_unit(self, data_unit_description=None, data_unit=None):
        """ creates a data unit object and initially imports data specified in data_unit_description """
        if data_unit!=None:
            du = data_unit
        else:
            du = DataUnit(pilot_data=self, 
                      data_unit_description=data_unit_description)
        self.data_unit_urls.append(du.get_url())
        du.add_pilot_data(self)
        return du
   
    
    def list_data_units(self):
        """ List all data units of Pilot Data """
        return self.data_unit_urls          
    
    
    def get_state(self):
        """ Return current state of Pilot Data """
        return self.__filemanager.get_state()
    
    
    def get_du(self, du_url):
        """ Returns Data Unit if part of Pilot Data """
        if self.data_unit_urls.count(du_url)>0:
            du = DataUnit(du_url=du_url)
            return du        
        return None
    
    
    def wait(self):
        """ Wait until PD enters a final state (Done, Canceled or Failed).""" 
        while 1:
            finish_counter=0
            result_map = {}
            for du_url in self.data_units_urls: 
                du = DataUnit(du_url=du_url)
                du.wait()
                state = du.get_state()           
                #state = job_detail["state"]                
                if result_map.has_key(state)==False:
                    result_map[state]=1
                else:
                    result_map[state] = result_map[state]+1
                if self.__has_finished(state)==True:
                    finish_counter = finish_counter + 1                   
            logger.debug("PD ID: " + str(self.id) + " Total DUs: %s States: %s"%(len(self.data_units_urls), str(result_map)))
            if finish_counter == len(self.data_units_urls):
                break
            time.sleep(2)

    
    def export_du(self, du, target_url):
        """ Export Data Unit to a local directory """
        if target_url.startswith("/") and os.path.exists(target_url)==False:
            os.mkdir(target_url)
        logger.debug("Export Data-Unit to %s"%target_url)
        self.__filemanager.get_du(du, target_url)
            
                
    def put_du(self, du):
        """Copy Data Unit to Pilot Data"""
        logger.debug("Put DU: %s to Pilot-Data: %s"%(du.id,self.service_url))
        self.__filemanager.create_du(du.id)
        self.__filemanager.put_du(du)
        self.data_unit_urls.append(du.get_url())
        CoordinationAdaptor.update_pd(self)
        
        
    def remove_du(self, du):
        """ Remove Data Unit from Pilot Data """
        if self.data_unit_urls.count(du.get_url())>0:
            self.__filemanager.remove_du(du)
            self.data_unit_urls.remove(du.get_url())
        CoordinationAdaptor.update_pd(self)
        
    
    def copy_du(self, du, pd_new):
        """ Copy DataUnit to another Pilot Data """
        pd_new.create_du(du)
        self.__filemanager.copy_du(du, pd_new)
        
        # update meta data at pd_new
        #pd_new.data_units[du.id] = du
        pd_new.data_unit_urls.append(du.get_url())
        CoordinationAdaptor.update_pd(pd_new)
        
    
    # END API methods
    ###########################################################################
    # Auxillary Methods

    def create_du(self, du):
        """ Create a new Data Unit within Pilot """
        self.__filemanager.create_du(du.id)
  
  
    def __initialize_pilot_data(self):
        
        if self.pilot_data_description!=None:
            self.service_url=self.pilot_data_description["service_url"]
            if self.pilot_data_description.has_key("size"):
                self.size = self.pilot_data_description["size"]
            
            # initialize file adaptor
            if self.service_url.startswith("ssh:"):
                logger.debug("Use SSH backend")
                self.__filemanager = SSHFileAdaptor(self.service_url,
                                                    self.security_context, 
                                                    self.pilot_data_description)
            elif self.service_url.startswith("http:"):
                logger.debug("Use WebHDFS backend")
                self.__filemanager = WebHDFSFileAdaptor(self.service_url)
            elif self.service_url.startswith("go:"):
                logger.debug("Use Globus Online backend")
                self.__filemanager = GlobusOnlineFileAdaptor(self.service_url)
            elif self.service_url.startswith("gs:"):
                logger.debug("Use Google Cloud Storage backend")
                self.__filemanager = GSFileAdaptor(self.service_url, self.security_context)
            elif self.service_url.startswith("irods:"):
                logger.debug("Use iRods Storage backend")
                self.__filemanager = iRodsFileAdaptor(self.service_url, self.security_context)
            elif self.service_url.startswith("s3:") \
                or self.service_url.startswith("walrus:") \
                or self.service_url.startswith("swift:"):
                logger.debug("Use Amazon S3/Eucalyptus Walrus/SWIFT Storage backend")
                self.__filemanager = S3FileAdaptor(self.service_url, 
                                                   self.security_context, 
                                                   self.pilot_data_description)
            else:
                raise PilotError("No File Plugin found.")
            
            self.__filemanager.initialize_pilotdata()
            self.__filemanager.get_pilotdata_size()
            
            # Update security context
            self.security_context = self.__filemanager.get_security_context()
            

    def __get_pd_id(self, pd_url):
        start = pd_url.index(self.PD_ID_PREFIX)
        end =pd_url.index("/", start)
        return pd_url[start:end]


    
    def to_dict(self):
        """ Internal method that returns a dict with all data contained in this Pilot Data"""
        pd_dict = {}
        pd_dict["id"]=self.id
        pd_dict["url"]=self.url
        pd_dict["pilot_data_description"]=self.pilot_data_description
        logger.debug("PilotData Dictionary: " + str(pd_dict))
        return pd_dict
    
    
    def __repr__(self):
        """Returns Pilot Data URL"""
        return self.service_url
    
    
    def __has_finished(self, state):
        state = state.lower()
        if state=="running" or state=="failed" or state=="canceled":
            return True
        else:
            return False
    
    @classmethod
    def create_pilot_data_from_dict(cls, pd_dict):
        """Restore Pilot Data from dictionary"""
        pd = PilotData()
        for i in pd_dict.keys():
            pd.__setattr__(i, pd_dict[i])
        pd.__initialize_pilot_data()
        logger.debug("created pd " + str(pd))
        return pd
   
    
###############################################################################
COORDINATION_URL = "redis://localhost"

class PilotDataService(PilotDataService):
    """ B{PilotDataService (PDS).}
    
        Factory for creating Pilot Data.
    
    """
    
    PDS_ID_PREFIX="pds-"
    
    def __init__(self, coordination_url=COORDINATION_URL, pds_url=None):
        """ Create a PilotDataService

            Keyword arguments:
            pds_id -- restore from pds_id
        """        
        self.pilot_data={}
        CoordinationAdaptor.configure_base_url(coordination_url)
        if pds_url == None:
            self.id = self.PDS_ID_PREFIX + str(uuid.uuid1())
            application_url = CoordinationAdaptor.get_base_url(application_id)
            self.url = CoordinationAdaptor.add_pds(application_url, self)
        else:
            self.id = self.__get_pds_id(pds_url)
       


    def create_pilot(self, pilot_data_description):
        """ Create a PilotData 

            Keyword arguments:
            pilot_data_description -- PilotData Description:: 
            
                {
                    'service_url': "ssh://<hostname>/base-url/",               
                    'size': "1000"
                }
            
            Return value:
            A PilotData object
        """
        pd = PilotData(pilot_data_service=self, 
                       pilot_data_description=pilot_data_description)
        self.pilot_data[pd.id]=pd
        
        # store pilot data in central data space
        CoordinationAdaptor.add_pd(self.url, pd)        
        return pd
    
    
    def get_pilot(self, pd_id):
        """ Reconnect to an existing pilot. """
        if self.pilot_data.has_key(pd_id):
            return self.pilot_data[pd_id]
        return None


    def list_pilots(self):
        """ List all PDs of PDS """
        return self.pilot_data.values()
    

    def cancel(self):
        """ Cancel the PilotDataService. Release all Pilot Data created by this service.
            
            Keyword arguments:
            None

            Return value:
            Result of operation
        """
        for i in self.pilot_data.values():
            i.cancel()
 
 
    def wait(self):
        """ Wait until all managed PD (of this Pilot Data Service) enter a final state""" 

        for i in self.pilot_data.values():
            i.wait()
 
 
    def get_url(self):
        """ Returns URL of Pilot Data Service """
        return self.url
 
    ###########################################################################
    # Non-API methods
    def to_dict(self):
        """ Return a Python dictionary containing the representation of the PDS 
            (internal method not part of Pilot API)        
        """
        pds_dict = self.__dict__
        pds_dict["id"]=self.id
        return pds_dict
 
 
    def __del__(self):
        """Releases all Pilot Data created by this Pilot Data Service."""
        self.cancel()         
            
    
    def __get_pds_id(self, pds_url):
        start = pds_url.index(self.PDS_ID_PREFIX)
        end =pds_url.index("/", start)
        return pds_url[start:end]

    
    def __restore_pd(self, pds_url):
        pd_list=CoordinationAdaptor.list_pd(pds_url) 
        for i in pd_list:
            pass
        

class DataUnit(DataUnit):
    """ B{DataUnit (DU).}
    
        This is the object that is returned by the ComputeDataService when a 
        new DataUnit is created based on a DataUnitDescription.

        The DataUnit object can be used by the application to keep track 
        of a DataUnit.

        A DataUnit has state, can be queried and can be cancelled.
    
        
    
        State model:
            - New: PD object created
            - Pending: PD object is currently updated  
            - Running: At least 1 replica of PD is persistent in a pilot data            
    """
    
    ## TODO
    # DU are stored as top-level objects in Redis:
    # redis://localhost/<application-id>/du-<id>
    #
    # In the future a DU can be possibly bound to multiple PD
    # Thus, it should be a top level entity
    # The lower levels of the hierarchy will only store references to the DU then
    
    
    DU_ID_PREFIX="du-"  

    def __init__(self, pilot_data=None, data_unit_description=None, du_url=None):
        """
            1.) create a new Pilot Data: pilot_data_service and data_unit_description required
            2.) reconnect to an existing Pilot Data: du_url required 
            
        """
        if du_url==None:
            self.id = self.DU_ID_PREFIX + str(uuid.uuid1())
            self.data_unit_description = data_unit_description        
            self.pilot_data=[]
            self.state = State.New
            self.data_unit_items=[]
            if self.data_unit_description.has_key("file_urls"):
                self.data_unit_items = DataUnitItem.create_data_unit_list(self, self.data_unit_description["file_urls"]) 

            self.url = None

            # register a data unit as top-level entry in Redis
            application_url = CoordinationAdaptor.get_base_url(application_id)
            self.url = CoordinationAdaptor.add_du(application_url, self)
            CoordinationAdaptor.update_du(self)
            
            # Deprecated
            # old method only allowed the creation of a du if a pd existed
            #if pilot_data!=None:
            #    # Allow data units that are not connected to a resource!
            #    self.url = CoordinationAdaptor.add_du(pilot_data.url, self)
            #    CoordinationAdaptor.update_du(self)
        else:
            self.id = DataUnit._get_du_id(du_url)
            self.url = du_url   
            logger.debug("Restore du: %s"%self.id)         
            self.__restore_state()
            
        self.transfer_threads=[]
               
            
    def cancel(self):
        """ Cancel the Data Unit. """
        self.state = State.Done    
        if len(self.pilot_data) > 0: 
            CoordinationAdaptor.update_du(self)

            
    def add_files(self, file_url_list=[]):
        """Add files referenced in list to Data Unit"""
        self._update_state(State.Pending)
        item_list = DataUnitItem.create_data_unit_from_urls(None, file_url_list)
        for i in item_list:
            self.data_unit_items.append(i)
        CoordinationAdaptor.update_du(self)    
        if len(self.pilot_data) > 0: 
            for i in self.pilot_data:
                logger.debug("Update Pilot Data %s"%(i.get_url()))
                i.put_du(self)
        self._update_state(State.Running)
        CoordinationAdaptor.update_du(self)    
        
        
    def remove_files(self, file_urls):
        """Remove files from Data Unit (NOT implemented yet"""
        # TODO
        #self.data_unit_items.remove(input_data_unit)
        if len(self.pilot_data) > 0:
            CoordinationAdaptor.update_du(self)

    
    def list_pilot_data(self):
        pd_urls = []
        for i in self.pilot_data:
            pd_urls.append(i.get_url())
        return pd_urls
    
    
    def list(self):
        """ List all items contained in DU 
            {
                "filename" : { 
                                "pilot_data" : [url1, url2],
                                "local" : url
                             }
            }        
        """        
        self.__refresh()
        base_urls = [i.url_for_du(self) for i in self.get_pilot_data()]
        result_dict = {}
        for i in self.data_unit_items:
            logger.debug("Process file: %s"%(i.filename))
            result_dict[i.filename]={
                                    "pilot_data": [os.path.join(j, i.filename) for j in base_urls],
                                    "local": i.local_url
                                    }
        return result_dict
    
   
    
    def get_state(self):
        """ Return current state of DataUnit """
        # update remote state
        du_dict = CoordinationAdaptor.get_du(self.url)
        self.state = du_dict["state"]
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
        while True:
            self.state = self.get_state()
            if self.state==State.Running or self.state==State.Failed:
                break
            logger.debug("Waiting DU %s State: %s"%(self.get_url(), self.state))
            time.sleep(2)
    
    
    def add_pilot_data(self, pilot_data):
        """ add this DU (self) to a certain pilot data 
            data will be moved into this data
        """
        transfer_thread=threading.Thread(target=self.__add_pilot_data, args=[pilot_data])
        transfer_thread.start()        
        self.transfer_threads.append(transfer_thread)
        
    
    def get_pilot_data(self):
        """ get a list of pilot data that have a copy of this PD """
        self.__restore_state()
        return self.pilot_data
    
    
    def export(self, target_url):
        """ simple implementation of export: 
                copies file from first pilot data to local machine
        """
        if self.get_state()!=State.Running:
            self.wait()
        
        if len(self.pilot_data) > 0:
            # Search for PD that is close to local machine
            local_hostname=socket.getfqdn()
            max_score=0
            best_pd=None
            for pd in self.pilot_data:
                pd_host = SAGAUrl(pd.service_url).host
                pd_score = difflib.SequenceMatcher(a=pd_host, b=local_hostname).ratio()
                logger.debug("Export locality compute score: Localhost: %s PD at: %s Score: %s"%(local_hostname, pd_host, pd_score))
                if pd_score > max_score:
                    best_pd=pd
                    max_score=pd_score
                
                #pd_domain = tldextract.extract(pd.service_url).domain
                #local_domain = tldextract.extract(socket.getfqdn()).domain
                
            if best_pd!=None:
                logger.debug("Export from: %s"%(best_pd.service_url))
                best_pd.export_du(self, target_url)
                return
                
            # No PD found. Utilize default PD
            logger.debug("Export from random PD")
            self.pilot_data[0].export_du(self, target_url)
        else:
            logger.error("No Pilot Data for PD found")
    
    
    def get_url(self):
        """ Return URL that can be used to reconnect to Data Unit """
        return self.url
    
    
    
    ###########################################################################
    # BigData Internal Methods
    def to_dict(self):
        """ Internal method that returns a dict with all data contained in this DataUnit"""
        du_dict = self.__dict__
        du_dict["id"]=self.id
        return du_dict        
    
    
    def _update_state(self, state):
        """ Internal method for updating state"""
        self.state=state
        logger.debug("Update DU: "+ str(self.url) +  " state: " + state)
        CoordinationAdaptor.update_du_state(self, state)
        logger.debug("Updated DU: "+ str(self.url) +  " New state: " + self.get_state())

    
    def __add_pilot_data(self, pilot_data):
        logger.debug("DU add_pilot_data: add DU to pilot data in Thread")
        self._update_state(State.Pending)
        if len(self.pilot_data) > 0: # copy files from other pilot data
            self.pilot_data[0].copy_du(self, pilot_data)
        else: # copy files from original location
            pilot_data.put_du(self)
        logger.debug("DU add_pilot_data: Copy/Put DU to pilot data successfull")    
        self.pilot_data.append(pilot_data)
        self._update_state(State.Running)
        logger.debug("DU add_pilot_data: Updated State")
        #self.url = CoordinationAdaptor.add_du(pilot_data.url, self)
        CoordinationAdaptor.update_du(self)
            
        
    @classmethod    
    def _get_du_id(cls, du_url):
        try:
            start = du_url.index(cls.DU_ID_PREFIX)
            end = du_url.find("/", start)
            if end==-1:
                end = du_url.find("?", start)
            if end==-1:
                end = len(du_url)
            return du_url[start:end]
        except:
            logger.error("No valid PD URL")
        return None
    
    
    def __refresh(self):
        """ Update list of data units items 
            from coordination service """
        try:
            if self.url != None:
                du_dict = CoordinationAdaptor.get_du(self.url)
                data_unit_dict_list = eval(du_dict["data_unit_items"])
                self.data_unit_items = [DataUnitItem.create_data_unit_from_dict(i) for i in data_unit_dict_list]
        except:
            logger.warn("Refresh of DU %s failed"%(self.get_url()))
            
        
    def __restore_state(self):
        du_dict = CoordinationAdaptor.get_du(self.url)
        # Restore Data Unit
        self.data_unit_description = eval(du_dict["data_unit_description"])
        self.state = du_dict["state"]
        
        # Restore DataUnitItems
        data_unit_dict_list = eval(du_dict["data_unit_items"])
        self.data_unit_items = [DataUnitItem.create_data_unit_from_dict(i) for i in data_unit_dict_list]
        
        # restore Pilot Data
        pd_list = eval(du_dict["pilot_data"])
        self.pilot_data = [] 
        for i in pd_list:
            logger.debug("PD: "+str(i)) 
            pd = PilotData(pd_url=str(i))
            self.pilot_data.append(pd) 


    def __repr__(self):        
        return "PD: " + str(self.url) 
        + " \nData Units: " + str(self.data_unit_items)
        + " \nPilot Data: " + str(self.pilot_data)
    
    

class DataUnitItem(object):
    """ DataUnitItem """
    DUI_ID_PREFIX="dui-"  
   
    def __init__(self, pd=None, local_url=None):        
        if local_url!=None:
            self.id = self.DUI_ID_PREFIX + str(uuid.uuid1())
            self.local_url = local_url   
            self.filename =  os.path.basename(local_url)    
            #if pd != None:
            #    self.url = pd.url + "/" + self.filename
        
        
    @classmethod    
    def __exists_file(cls, url):   
        """ return True if file at url exists. Otherwise False """
        file_url = SAGAUrl(url)
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
        
    
    def __repr__(self):
        return str(self.__dict__) 
        
        
    ###########################################################################
    # Auxiliary Methods
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
        du_item_list = []    
        for i in urls:            
            if cls.__exists_file(i):
                du = DataUnitItem(pd, i)
                du_item_list.append(du)
    
        return du_item_list
    
    
    @classmethod
    def create_data_unit_from_dict(cls, du_dict):
        du = DataUnitItem()
        logger.debug("Restore DU: " + str(du_dict))
        for i in du_dict.keys():
            logger.debug("Set attribute: %s", i)
            du.__setattr__(i, du_dict[i])
        return du
    
    
    def to_dict(self):
        du_dict = self.__dict__
        du_dict["id"]=self.id
        return du_dict
    
###################################################################################################    
# Tests
# Auxilliary testing methods
def __get_pd_url(du_url):
    url = du_url[:du_url.index(":du-")]
    return url

def __get_du_id(du_url):
    du_id = du_url[du_url.index("du-"):]
    return du_id

# Tests
def test_pd_reconnect():
    du_url = "redis://localhost/bigdata:pds-f31a670c-e3f6-11e1-afaf-705681b3df0f:pd-f31c47b8-e3f6-11e1-af44-705681b3df0f:du-f4debce8-e3f6-11e1-8399-705681b3df0f"
    pd_url = __get_pd_url(du_url)
    pd = PilotData(pd_url=pd_url)
    print str(pd.list_data_units())
    du = pd.get_du(du_url)
    
    #du = DataUnit(du_url="redis://localhost/bigdata:pds-32d63b2e-df05-11e1-a329-705681b3df0f:pd-37674138-df05-11e1-80d0-705681b3df0f:du-3b8d428c-df05-11e1-af2a-705681b3df0f")
    logger.debug(str(du.list()))


def test_du_reconnect():
    du_url = "redis://localhost/bigdata:du-1d1b7078-229f-11e2-834e-705681b3df0f"
    du = DataUnit(du_url=du_url)
    logger.debug(str(du.list()))
    du.export("/tmp/export-test")
    
    
def test_data_unit_add_file():
    pilot_data_service = PilotDataService(coordination_url="redis://localhost/")
    pilot_data_description = {
                                "service_url": "ssh://localhost/tmp/pilot-" + str(uuid.uuid1()),
                                "size": 100                                   
                             }
    pd = pilot_data_service.create_pilot(pilot_data_description=pilot_data_description)
    
    # create data unit for output data
    output_data_unit_description = {
         "file_urls": [], 
         "file_url_patterns": ["test.txt"]                             
    }
    output_data_unit = pd.submit_data_unit(output_data_unit_description)
    output_data_unit.wait()
    logger.debug("Output DU: " + output_data_unit.get_url())
    pd_reconnect_url = pd.get_url()
    du_url = output_data_unit.get_url()
    pd_reconnect = PilotData(pd_url=pd_reconnect_url)
    du_reconnect = pd_reconnect.get_du(du_url)
    du_reconnect.add_files(["test.txt"])
    
    
    
class Lock(object):
    def __init__(self, key, redis, expires=60, timeout=10):
        """
        Distributed locking using Redis SETNX and GETSET.

        Usage::

            with Lock('my_lock'):
                print "Critical section"

        :param  expires     We consider any existing lock older than
                            ``expires`` seconds to be invalid in order to
                            detect crashed clients. This value must be higher
                            than it takes the critical section to execute.
        :param  timeout     If another client has already obtained the lock,
                            sleep for a maximum of ``timeout`` seconds before
                            giving up. A value of 0 means we never wait.
        """

        self.key = key
        self.timeout = timeout
        self.expires = expires
        self.redis = redis
        
    def __enter__(self):
        timeout = self.timeout
        while timeout >= 0:
            expires = time.time() + self.expires + 1

            if self.redis.setnx(self.key, expires):
                # We gained the lock; enter critical section
                return

            current_value = self.redis.get(self.key)

            # We found an expired lock and nobody raced us to replacing it
            if current_value and float(current_value) < time.time() and \
                self.redis.getset(self.key, expires) == current_value:
                    return

            timeout -= 1
            time.sleep(1)

        raise LockTimeout("Timeout whilst waiting for lock")

    def __exit__(self, exc_type, exc_value, traceback):
        self.redis.delete(self.key)

class LockTimeout(BaseException):
    pass
    

if __name__ == "__main__":
    #test_data_unit_add_file()
    test_du_reconnect()
    
