'''
Google Storage based File Transfer Implementation
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

from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from oauth2client.file import Storage
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import Credentials
from oauth2client.tools import run
import httplib2
import urllib

"""
AN OAUTH2 Client Id must be created at the Google API console at:

https://code.google.com/apis/console/

=> API Access

More information with respect to OAUTH: https://developers.google.com/compute/docs/api/how-tos/authorization
"""
OAUTH2_CLIENT_ID='1004462711324-55akehip32m59u6omdfrt9s8u8ehb0hm.apps.googleusercontent.com'
OAUTH2_CLIENT_SECRET='EIMML1W7anu0XijVghws0DY-'

GS_PROJECT_ID="1004462711324"


class GSFileAdaptor(object):
    """ BigData File Management for Pilot Data """
   
    
    def __init__(self, service_url, security_context=None):        
        # Initializations of instance variables
        self.service_url = service_url
        self.bucket_name = self.__get_bucket_name(service_url)
        self.__state=State.New
                
        # Do OAUTH authentication
        if security_context!=None:
            logger.debug("Attempt to restore credentials from security context: " + str(security_context))
            self.credentials = Credentials.new_from_json(security_context)
        else:
            storage = Storage('gce.dat')
            self.credentials = storage.get()
        if self.credentials is None or self.credentials.invalid == True:
            logger.debug("No valid credential found. Run new OAuth authentication round...")
            flow = OAuth2WebServerFlow(
                                       client_id=OAUTH2_CLIENT_ID,
                                       client_secret=OAUTH2_CLIENT_SECRET,
                                       scope=['https://www.googleapis.com/auth/devstorage.full_control', 
                                              'https://www.googleapis.com/auth/compute'],
                                       user_agent='bigjob-client/1.0')

            self.credentials = run(flow, storage)

    
    def get_security_context(self):
        """ Returns security context that needs to be available on the distributed
            node in order to access this Pilot Data """
        return self.credentials.to_json()
    
        
    def initialize_pilotdata(self):
        # check whether directory exists
        self.__state=State.Running
        request_dict = {
                        "id":self.bucket_name,
                        "projectId":GS_PROJECT_ID
        }       
        logger.debug(str(request_dict)) 
        try:
            gs = self.__get_api_client()[0]
            gs.buckets().insert(body=request_dict).execute()
        except:
            logger.debug("Error creating bucket: " + self.bucket_name)
            pass # Do nothing if bucket already exists
                
        
    def get_pilotdata_size(self):
        # unlimited size
        return None
    
    
    def delete_pilotdata(self):
        self.__state=State.Done
    
        
    def get_state(self):
        return self.__state
        
            
    def create_du(self, du_id):
        gs = self.__get_api_client()[0]
        o = gs.objects().insert(bucket=self.bucket_name, name=str(du_id)+"/du_info",
                                body={'media': {
                                                "contentType":"text/ascii",
                                                "data": du_id    
                                                } 
                                      }                                
                                ).execute()
        logger.debug("Created GS: " + str(o))
        
                
    def put_du(self, du):
        logger.debug("Copy DU to Google Storage")
        du_items = du.list()
        for i in du_items.keys():   
            try:
                local_filename=du_items[i]["local"]
                remote_path = os.path.join(str(du.id), os.path.basename(local_filename))
                self._put_file(local_filename, remote_path)
            except:
                logger.debug("Could not copy file: " + (str(i)))
            
    
    def copy_du(self, du, pd_new):
        pass
        
    
    def get_du(self, du, target_url):
        du_id=du.id
        logger.debug("Get DU: " + str(du_id))
        gs = self.__get_api_client()[0]
        result = gs.objects().list(bucket=self.bucket_name, prefix=du_id).execute() 
                            #delimiter="/",
                            #prefix=[du_id]).execute()   
        logger.debug("Result: " + str(result))
        for i in result["items"]:
            full_filename = i["name"]
            self._get_file(full_filename, os.path.join(target_url, os.path.basename(full_filename)))
        
                
    def remove_du(self, du):
        self.__remove_directory(os.path.join(self.bucket_name, du.id))
    
    
    ###########################################################################
    # Pure File Management APIs
    def _put_file(self, source, target):
        logger.debug("Put file: %s to %s"%(source, target))
        gs = self.__get_api_client()[0]
        #media = MediaFileUpload(source, 
        #                        resumable=False)
        o = gs.objects().insert(bucket=self.bucket_name, 
                                name=target,
                                media_body=source).execute()            
        logger.debug("Put file result: %s"%str(o))
    
    
    def _get_file(self, source, target):
        logger.debug("GET file: %s to %s"%(source, target))
        gs, http = self.__get_api_client()
        f = gs.objects().get(bucket=self.bucket_name, 
                             object=source).execute()            
        logger.debug("Get file result: %s"%str(f))
        downloadUrl = f["media"]['link']
        if downloadUrl:
            response, content = http.request(downloadUrl)
            logger.debug("Download file response: %d"%(response.status))
            with open(target, 'wb') as f:
                f.write(content)
        
    
    def transfer(self, source_url, target_url):
        pass
    
    def create_remote_directory(self, target_url):
        return True
    
    
    def get_path(self, target_url):
        result = urlparse.urlparse(target_url)
        target_path = result.path
        return target_path
        
        
    ###########################################################################
    # Auxiliary functions
    def __get_api_client(self):
        http = httplib2.Http()
        http = self.credentials.authorize(http)
        gs = build("storage", "v1beta1", http=http)
        return gs, http
    
    
    def __get_bucket_name(self, service_url):
        bucket_name = service_url.replace("gs://", "")
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
    gs = GSFileAdaptor("gs://pilot-data-bucket-1234")
    gs.initialize_pilotdata()
    gs._put_file("test-random.exe", "test.exe")
    gs._get_file("test.txt", "test2.txt")
    gs.get_du(None, ".")