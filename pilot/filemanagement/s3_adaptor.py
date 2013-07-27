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

# This is for local debugging!
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import saga
from pilot.api import State
from bigjob import logger

##################

from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.key import Key
from boto.s3.connection import Location

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
    """ BigData File Management for Pilot Data 
        Supports pilot data on top of S3 and Eucalyptus Walrus
        
            s3://
            walrus://<endpoint-ip>    
            
        Amazon S3 supported regions:
        
            Default is an empty string "" => us-east-1
        
            'ap-northeast-1'
            'ap-southeast-1'
            'ap-southeast-2'
            'EU'
            'sa-east-1'
            'us-west-1'
            'us-west-2'
        
    """
    
   
    
    def __init__(self, resource_url, security_context=None, pilot_data_description=None):        
        self.resource_url = saga.Url(resource_url)
        self.bucket_name = self.__get_bucket_name(resource_url)
        self.__state=State.New
        self.pilot_data_description = pilot_data_description
        aws_access_key_id=None
        aws_secret_access_key=None
        
        # try to recover key from pilot_data_description
        if self.pilot_data_description!=None and\
           self.pilot_data_description.has_key("access_key_id") and \
           self.pilot_data_description.has_key("secret_access_key"):
            aws_access_key_id=self.pilot_data_description["access_key_id"]
            aws_secret_access_key=self.pilot_data_description["secret_access_key"]
        
        # try to recover key from security context
        if security_context!=None:
            logger.debug("Attempt to restore credentials from security context: " + str(security_context))
            security_context = eval(security_context)
            aws_access_key_id=security_context["aws_access_key_id"]
            aws_secret_access_key=security_context["aws_secret_access_key"]
    
        self.s3_conn=None
        if self.resource_url.scheme == "walrus" or self.resource_url.scheme == "swift":
            calling_format=OrdinaryCallingFormat()
            logger.debug("Access Key: %s Secret: %s Host: %s"%(aws_access_key_id,
                                                               aws_secret_access_key,
                                                               self.resource_url.host)
                         )
            port = 8773
            if self.resource_url.port!=None:
                port = self.resource_url.port
            
            path = "/"
            if self.resource_url.scheme == "walrus":
                path = "/services/Walrus"
                
            self.s3_conn = S3Connection(aws_access_key_id=aws_access_key_id,
                                        aws_secret_access_key=aws_secret_access_key,
                                        is_secure=False,
                                        host=self.resource_url.host,
                                        port=port,
                                        calling_format=calling_format,
                                        path=path)
        else: # s3:// urls
            self.s3_region = None
            # Region specifier according to Amazon API:
            # http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETlocation.html
            if self.pilot_data_description.has_key("region"):
                self.s3_region =  self.pilot_data_description["region"]
           
            self.s3_conn = S3Connection(
                                        aws_access_key_id, 
                                        aws_secret_access_key, 
                                        )
      
    
    
    def get_security_context(self):
        """ Returns security context that needs to be available on the distributed
            node in order to access this Pilot Data """
        return {"aws_access_key_id": self.s3_conn.aws_access_key_id,
                "aws_secret_access_key": self.s3_conn.aws_secret_access_key}

                       
        
    def initialize_pilotdata(self):
        # Create bucket
        try:
            if self.s3_region==None:
                logger.debug("Use default S3 region.")
                self.s3_region = "" # Default for US East
            self.bucket = self.s3_conn.create_bucket(self.bucket_name, location=self.s3_region)
        except:
            # bucket already exists
            #self.__print_traceback()
            self.bucket = self.s3_conn.get_bucket(self.bucket_name)
            
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
        logger.debug("Copy DU to S3/Walrus")
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
        logger.debug("Put file: %s to %s"%(source, target))
        if self.__starts_with_valid_prefix(source):
            logger.debug("Copy file from S3/Walrus")
            source_bucket_name = self.__get_bucket_name(source)
            source_key_name = self.__get_key_name(source)
            self.bucket.copy_key(target, source_bucket_name, source_key_name)
            #k = Key(source_bucket_name)
            #k.copy(self.bucket_name, target)
        else:
            logger.debug("Copy file from Local")
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
    def __starts_with_valid_prefix(self, url):
        valid_prefix=["s3", "walrus"]
        result = False
        for i in valid_prefix:
            result = url.startswith(i)
            if result == True:
                break
        return result
    
    def __get_bucket_name(self, resource_url):
        surl = saga.Url(resource_url)
        if surl.scheme.startswith("s3"):
            bucket_name = resource_url.replace("s3://", "")
            try:
                bucket_name = bucket_name[:bucket_name.index("/")]
            except:
                pass
            #bucket_name = bucket_name.replace("/", "")
        else:
            bucket_name = surl.path[1:]
        return bucket_name
        
   
    def __get_key_name(self, resource_url):
        surl = saga.Url(resource_url)
        # get path out of URL
        if surl.scheme.startswith("s3"):
            bucket_name = resource_url.replace("s3://", "")            
        else:
            bucket_name = surl.path[1:]
        
        # get key path out of URL
        try:
            key_name = bucket_name[bucket_name.index("/")+1:]
        except:
            pass
        
        return key_name
    
   
    def __print_traceback(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "*** print_tb:"
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        print "*** print_exception:"
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)
    
    
def test_walrus():
    s3 = S3FileAdaptor("walrus://149.165.146.135/pilot-data-c4eb26eb-ed0c-11e1-ac98-705681b3df0f", 
                       pilot_data_description={ "access_key_id":"8MCXRAMXMHDYKWNKXZ8WF",
                                                "secret_access_key":"YrcUqSw2Arxshrh3ZtenkxerWwCWdMTKvZYoLPAo" })
    s3.initialize_pilotdata()
    s3._put_file("test.txt", "du-7370d7b5-ed0b-11e1-95df-705681b3df0f/test.txt")
    s3._get_file("du-7370d7b5-ed0b-11e1-95df-705681b3df0f/test.txt", "test2.txt")
    s3.get_du("du-7370d7b5-ed0b-11e1-95df-705681b3df0f", ".")
    
def test_swift():
    s3 = S3FileAdaptor("swift://149.165.146.50:3333/pilot-data-c4eb26eb-ed0c-11e1-ac98-705681b3df0f", 
                       pilot_data_description={ "access_key_id":"f9716a49c92a4a4cbedb6aba5e78d682",
                                                "secret_access_key":"bcdff54b7fe94d63b4412c762e823a84" })
    s3.initialize_pilotdata()
    s3._put_file("test.txt", "du-7370d7b5-ed0b-11e1-95df-705681b3df0f/test.txt")
    s3._get_file("du-7370d7b5-ed0b-11e1-95df-705681b3df0f/test.txt", "test2.txt")
    s3.get_du("du-7370d7b5-ed0b-11e1-95df-705681b3df0f", ".")
   
def test_s3import():
    s3 = S3FileAdaptor("s3://pilot-data-andre-test-create-from-s3-url", 
                       pilot_data_description={ "access_key_id":"AKIAJPGNDJRYIG5LIEUA",
                                                "secret_access_key":"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" })
    s3.initialize_pilotdata()
    s3._put_file("s3://pilot-data-05d88e40-f65b-11e1-a327-00215ec9e3ac/du-3624837e-f66f-11e1-a327-00215ec9e3ac/WRT54GS_UG_WEB_20070529.pdf", "bla/test.pdf")

def test_s3import_via_pilotapi():
    COORDINATION_URL="redis://localhost:6379"
    from pilot import PilotComputeService, PilotDataService, ComputeDataService, State
    pilot_data_service = PilotDataService(coordination_url=COORDINATION_URL)
    
    ###################################################################################################
    # Pick one of the Pilot Data Descriptions below    
    
    pilot_data_description_aws={
                                "service_url": "s3://pilot-data-andre-workflow",
                                "size": 100,   
                                "affinity_datacenter_label": "us-east-1",              
                                "affinity_machine_label": ""    ,
                                "access_key_id": "AKIAJPGNDJRYIG5LIEUA",
                                "secret_access_key":"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",                                                         
                                }

    pd = pilot_data_service.create_pilot(pilot_data_description=pilot_data_description_aws)
     
    data_unit_description = {
                              "file_urls": ['s3://pilot-data-cec5d816-fa8f-11e1-ab5e-e61f1322a75c/du-67b4c762-fa90-11e1-ab5e-e61f1322a75c/ip-10-84-173-21512MB_2.input-chunk-02'],
                              "affinity_datacenter_label": "us-east-1",              
                              "affinity_machine_label": ""
                             }    
      
    # submit pilot data to a pilot store 
    input_data_unit = pd.submit_data_unit(data_unit_description)
    input_data_unit.wait()
    

if __name__ == "__main__":
    test_s3import_via_pilotapi()
    
