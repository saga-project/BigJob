'''
Amazon S3 based File Transfer Implementation
'''
import urlparse

import errno
import sys
import os
import stat
import logging
import traceback
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from pilot.api import State
from bigjob import logger

##################

from boto.s3.connection import S3Connection

# Authentication
# Please use ~/.boto file to configure your security credentials (if possible)
# see http://boto.readthedocs.org/en/latest/boto_config_tut.html
# 
# [Credentials]
# aws_access_key_id = <your access key>
# aws_secret_access_key = <your secret key>
#
# Alternatively you can use these two variables
AWS_ACCESS_KEY_ID=None
AWS_SECRET_ACCESS_KEY=None

class S3FileAdaptor(object):
    """ BigData File Management for Pilot Data """
    
   
    
    def __init__(self, service_url):        
        self.service_url = service_url
        result = urlparse.urlparse(service_url)
        self.host = result.netloc
        self.path = result.path
        self.__state=State.New
        self.s3_conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
      
    
    def get_security_context(self):
        """ Returns security context that needs to be available on the distributed
            node in order to access this Pilot Data """
        return None
                       
        
    def initialize_pilotdata(self):
        # check whether directory exists
        self.__state=State.Running
        
        # Create bucket
        self.s3_conn.create_bucket(self.path)
        
        
    def get_pilotdata_size(self):
        # unlimited size
        return None
    
    
    def delete_pilotdata(self):
        self.__state=State.Done
    
        
    def get_state(self):
        return self.__state
        
            
    def create_du(self, du_id):
        du_dir = os.path.join(self.path, str(du_id))
        logger.debug("mkdir: " + du_dir)
        
                
    def put_du(self, du):
        pass       
                
    
    def copy_du_to_url(self, du,  local_url, remote_url):
        pass

        

    def copy_du(self, du, pd_new):
        remote_url = pd_new.service_url + "/" + str(du.id)
        local_url =  self.service_url  + "/" + str(du.id)
        self.copy_du_to_url(du, local_url, remote_url)  
        
    
    def get_du(self, du, target_url):
        remote_url = target_url
        local_url =  self.service_url  + "/" + str(du.id)
        self.copy_du_to_url(du, local_url, remote_url)  
        
        
    def remove_du(self, du):
        self.__remove_directory(os.path.join(self.path, du.id))
    
    
    ###########################################################################
    # Pure File Management APIs
        
    def transfer(self, source_url, target_url):
        pass
    
    def create_remote_directory(self, target_url):
        return True
    
    
    def get_path(self, target_url):
        result = urlparse.urlparse(target_url)
        target_query = result.path
        
        
    ###########################################################################
    
   
    def __print_traceback(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "*** print_tb:"
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        print "*** print_exception:"
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)
    
    
if __name__ == "__main__":
    gs = GSFileAdaptor("gs://google.com/pilot-data-bucket-1234")
    gs.initialize_pilotdata()
    