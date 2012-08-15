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
from boto.s3.key import Key

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
    
   
    
    def __init__(self, service_url, security_context=None):        
        self.service_url = service_url
        self.bucket_name = self.__get_bucket_name(service_url)
        self.__state=State.New
        aws_access_key_id=AWS_ACCESS_KEY_ID
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        if security_context!=None:
            logger.debug("Attempt to restore credentials from security context: " + str(security_context))
            security_context = eval(security_context)
            aws_access_key_id=security_context["aws_access_key_id"]
            aws_secret_access_key=security_context["aws_secret_access_key"]
        self.s3_conn = S3Connection(aws_access_key_id, aws_secret_access_key)
      
    
    def get_security_context(self):
        """ Returns security context that needs to be available on the distributed
            node in order to access this Pilot Data """
        return {"aws_access_key_id": self.s3_conn.aws_access_key_id,
                "aws_secret_access_key": self.s3_conn.aws_secret_access_key}

                       
        
    def initialize_pilotdata(self):
        # Create bucket
        self.bucket = self.s3_conn.create_bucket(self.bucket_name)
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
        k = Key(self.bucket)
        k.key = str(du_id)+"/du_info"
        k.set_contents_from_string(du_id)
                 
                 
    def put_du(self, du):
        logger.debug("Copy DU to S3")
        du_items = du.list()
        for i in du_items.keys():     
            local_filename=du_items[i]["local"]
            remote_path = os.path.join(str(du.id), local_filename)
            self._put_file(local_filename, remote_path)
                
    
    def get_du(self, du, target_url):
        du_id = du.id
        logger.debug("Get DU: " + str(du_id))
        result = self.bucket.list(prefix=du_id)
        logger.debug("Result: " + str(result))
        for key in result:
            full_filename = key.name
            print full_filename
            if not full_filename.endswith("/"):
                self._get_file(full_filename, os.path.join(target_url, os.path.basename(full_filename)))
        
   
    def copy_du(self, du, pd_new):
        remote_url = pd_new.service_url + "/" + str(du.id)
        local_url =  self.service_url  + "/" + str(du.id)
        self.copy_du_to_url(du, local_url, remote_url)  
        
    
        
        
    def remove_du(self, du):
        self.__remove_directory(os.path.join(self.path, du.id))
    
    
    ###########################################################################
    # Pure File Management APIs
    def _put_file(self, source, target):
        logger.debug("Put file: %s to %s"%(source, target))
        k = Key(self.bucket)
        k.key=target
        k.set_contents_from_filename(source)
        logger.debug("Put file result: %s"%source)
    
    
    def _get_file(self, source, target):
        logger.debug("GET file: %s to %s"%(source, target))
        k = self.bucket.get_key(source)
        k.key=source
        k.get_contents_to_filename(target)
        
         
    def transfer(self, source_url, target_url):
        pass
    
    def create_remote_directory(self, target_url):
        return True
    
                   
    ###########################################################################
    def __get_bucket_name(self, service_url):
        bucket_name = service_url.replace("s3://", "")
        bucket_name = bucket_name.replace("/", "")
        return bucket_name
        
   
   
    def __print_traceback(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "*** print_tb:"
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        print "*** print_exception:"
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)
    
    
    
if __name__ == "__main__":
    s3 = S3FileAdaptor("s3://pilot-data-bucket-1234", None)
    s3.initialize_pilotdata()
    s3._put_file("test.txt", "test.txt")
    s3._get_file("test.txt", "test2.txt")
    s3.get_du(None, ".")