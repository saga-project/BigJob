'''
HDFS File Transfer Implementation
'''
import urlparse

import errno
import sys
import os
import stat
import logging
import traceback
import time
from tempfile import NamedTemporaryFile

import saga
from pilot.api import State
from bigjob import logger

# from snakebite.client import Client
#from pywebhdfs.webhdfs import PyWebHdfsClient
from webhdfs.webhdfs import WebHDFS 
from pilot.api.api import PilotError



class HDFSFileAdaptor(object):
    """ Hadoop HDFS File Management for Pilot Data 
        Supports pilot data on top of S3 and Eucalyptus Walrus
        
            hdfs://localhost:9000
                    
    """
    
     
    def __init__(self, resource_url, security_context=None, pilot_data_description=None):        
        self.resource_url = saga.Url(resource_url)
        
        self.user = self.resource_url.username
        if self.user==None or self.user=="":
            raise PilotError("Username required in URL: %s"%(self.resource_url))
        self.host = self.resource_url.host
        if self.host==None or self.host=="":
            raise PilotError("Host required in URL: %s"%(self.resource_url))
        self.port = self.resource_url.port
        if self.port==None or self.port=="":
            raise PilotError("Port required in URL: %s"%(self.resource_url))
        
        self.path = self.resource_url.path    
        if self.path==None or self.path=="":
            raise PilotError("Path required in URL: %s"%(self.resource_url))
        
            
        #self.client = Client(self.host, self.port)
        self.client =  WebHDFS(self.host, self.port, self.user)
        self.__state=State.New
        self.pilot_data_description = pilot_data_description
              
    
    
    def get_security_context(self):
        """ Returns security context that needs to be available on the distributed
            node in order to access this Pilot Data """
        return {}

                       
        
    def initialize_pilotdata(self):
        # Create bucket
        try:
            self.client.mkdir(self.path)
        except:
            self.__print_traceback()
            
        self.__state=State.Running
       
        
    def get_pilotdata_size(self):
        # unlimited size
        return None
    
    
    def delete_pilotdata(self):
        self.__state=State.Done
    
        
    def get_state(self):
        return self.__state
        
            
    def create_du(self, du_id):
        logger.debug("create object: " + du_id)
        self.client.mkdir(os.path.join(self.path, du_id))
        f = NamedTemporaryFile(delete=True)
        f.write(str(du_id))
        f.flush()
        self.client.copyFromLocal(f.name, os.path.join(self.path, du_id, "du_info"))
        f.close()        
                 
                 
    def put_du(self, du):
        logger.debug("Copy DU to HDFS")
        du_items = du.list()
        for i in du_items.keys():     
            try:
                local_filename=du_items[i]["local"]
                remote_path = os.path.join(str(du.id), os.path.basename(local_filename))
                logger.debug("copy %s to %s"%(local_filename, remote_path))
                self._put_file(local_filename, remote_path)
            except:
                logger.debug("Could not copy: " + str(i))
        
                
    
    def get_du(self, du, target_url):
        #du_id = "du-7370d7b5-ed0b-11e1-95df-705681b3df0f"
        du_id = du.id
        logger.debug("Get DU: " + str(du_id))
        result = self.bucket.list(prefix=du_id)
        logger.debug("Result Bucket List: " + str(result))
        for key in result:
            logger.debug(str(key))
            full_filename = key.name
            if full_filename != None:
                logger.debug("Process file: " + full_filename)
                if not full_filename.endswith("/"):
                    self._get_file(full_filename, os.path.join(target_url, os.path.basename(full_filename)))
        
   
    def copy_du(self, du, pd_new):
        remote_url = pd_new.resource_url + "/" + str(du.id)
        local_url =  self.resource_url  + "/" + str(du.id)
        self.copy_du_to_url(du, local_url, remote_url)  
            
        
    def remove_du(self, du):
        self.__remove_directory(os.path.join(self.path, du.id))
    
    
    ###########################################################################
    # Pure File Management APIs
    def _put_file(self, source, target):
        logger.debug("Put file: %s to %s"%(source, os.path.join(self.path, target)))
        self.client.copyFromLocal(source, os.path.join(self.path, target))
        logger.debug("Put file completed. Result: %s"%source)
    
    
    def _get_file(self, source, target):
        source_path = os.path.join(self.path, source)
        logger.debug("GET file: %s to %s"%(source_path, target))
        self.client.copyToLocal(source_path, target)
        
         
    def transfer(self, source_url, target_url):
        pass
    
    def create_remote_directory(self, target_url):
        return True

    
   
    def __print_traceback(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "*** print_tb:"
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        print "*** print_exception:"
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)
    


    
def test_hdfs():
    hdfs = HDFSFileAdaptor("hdfs://luckow@localhost:50070/user/luckow/pilot-data-test", 
                       pilot_data_description={ })
    hdfs.initialize_pilotdata()
    hdfs._put_file("test.txt", "du-7370d7b5-ed0b-11e1-95df-705681b3df0f/test.txt")
    hdfs._get_file("du-7370d7b5-ed0b-11e1-95df-705681b3df0f/test.txt", "test2.txt")
    hdfs.get_du("du-7370d7b5-ed0b-11e1-95df-705681b3df0f", ".")
    

def test_pilotapi():
    COORDINATION_URL="redis://localhost:6379"
    from pilot import PilotComputeService, PilotDataService, ComputeDataService, State
    pilot_data_service = PilotDataService(coordination_url=COORDINATION_URL)
    
    ###################################################################################################
    # Pick one of the Pilot Data Descriptions below    
    
    pilot_data_description={"service_url": "hdfs://luckow@localhost:50070/user/luckow/pilot-data-andre",                                
                                }

    pd = pilot_data_service.create_pilot(pilot_data_description=pilot_data_description)
     
    data_unit_description = {
                             "file_urls": ['test.txt'],
                            }    
      
    # submit pilot data to a pilot store 
    input_data_unit = pd.submit_data_unit(data_unit_description)
    input_data_unit.wait()
    

if __name__ == "__main__":
    test_pilotapi()
    
