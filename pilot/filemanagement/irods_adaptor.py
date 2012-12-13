'''
iRods based File Transfer Implementation
'''
import urlparse

import errno
import sys
import os
import stat
import logging
import traceback
import time
import re

import pexpect

# This is for local debugging!
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import bliss.saga as saga
from pilot.api import State
from bigjob import logger



class iRodsFileAdaptor(object):
    """ BigData File Management for Pilot Data 
        Supports pilot data on top of iRods.
        
            Assumption: working iRods installation
        
            irods://localhost/${OSG_DATA}/?vo=<vo>&resource-group=<resource-group>
            
    """ 
    def __init__(self, resource_url, security_context=None, pilot_data_description=None):        
        self.resource_url = saga.Url(resource_url)
        query_string = self.resource_url.query
        self.localpath = self.resource_url.path
        self.vo = re.search("(?<=vo=)(.*)([&\b]{1})", query_string).group(1)
        self.resource_group = re.search("(?<=resource-group=)(.*)[&\b$]?", query_string).group(1)
        logger.debug("VO: %s, Resource Group: %s"%(self.vo, self.resource_group))
        self.is_local = self.__is_local()
        
    
    def __is_local(self):
        # test whether path contains environment variable
        match = re.search("\$\{(.*)\}", self.localpath)
        if match:
            env_var = match.group(1)
            logger.debug("Found: " + env_var)
            if os.environ.has_key(env_var):
                self.localpath=os.environ[env_var]
                return True
        return False
    
    
    def get_security_context(self):
        """ Returns security context that needs to be available on the distributed
            node in order to access this Pilot Data """
        return None
                       
        
    def initialize_pilotdata(self):
        pass
       
        
    def get_pilotdata_size(self):
        # unlimited size
        return None
    
    
    def delete_pilotdata(self):
        self.__state=State.Done
    
        
    def get_state(self):
        return self.__state
        
            
    def create_du(self, du_id):
        logger.debug("create iRods collection: " + du_id)
        if self.is_local:
            command = "mkdir %s"%(os.path.join(self.localpath, du_id))
        else:
            command = "imkdir %s"%(du_id)
        self.__run_command(command)

                 
    def put_du(self, du):
        logger.debug("Copy DU to iRod")
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
        if self.is_local:
            command = "cp -r %s %s"%(os.path.join(self.localpath, du_id), target_url)
        else:
            command = "iget -r %s %s"%(du_id, target_url)
        logger.debug(command)
        self.__run_command(command)
        if target_url==".":
            target_url = os.getcwd()
        command = "mv %s/* %s"%(os.path.join(target_url, du_id), target_url)
        self.__run_command(command)
        logger.debug("Finished Get DU")
        
   
    def copy_du(self, du, pd_new):
        remote_url = pd_new.resource_url + "/" + str(du.id)
        local_url =  self.resource_url  + "/" + str(du.id)
        self.copy_du_to_url(du, local_url, remote_url)  
            
        
    def remove_du(self, du):
        if self.is_local:
            command = "rm -rf %s"%(os.path.join(self.localpath, du.id))
        else:
            command = "irm %s"%du.id
        
        self.__run_command(command)
    
    
    ###########################################################################
    # Pure File Management APIs
    def _put_file(self, source, target):
        logger.debug("Put file: %s to %s"%(source, target))
        if self.is_local:
            command = "cp -r %s %s"%(os.path.join(source, target))
        else:
            command = "iput -f -R %s %s %s"%(self.resource_group, source, target)
        self.__run_command(command)
        home_directory= self.__run_command("ipwd")[0].strip()
        full_filename = os.path.join(home_directory, target)
        command = "irepl-osg -f %s -G %s"%(full_filename, self.resource_group)
        self.__run_command(command) 
         

    def transfer(self, source_url, target_url):
        pass
    
    
    def create_remote_directory(self, target_url):
        return True
    
                   
    ###########################################################################
    def __run_command(self, command):
        logger.debug(command)
        child = pexpect.spawn(command, timeout=None)
        output = child.readlines()
        logger.debug("Run %s Output: %s"%(command, str(output)))
        child.close()
        return output 

    def __print_traceback(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "*** print_tb:"
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        print "*** print_exception:"
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)
    
    
def test_irods():
    irods = iRodsFileAdaptor("irods://gw68/${OSG_DATA}/?vo=osg&resource-group=osgGridFtpGroup")
    irods.initialize_pilotdata()
    irods.create_du("du-7370d7b5-ed0b-11e1-95df-705681b3df0f")
    irods._put_file("test.txt", "du-7370d7b5-ed0b-11e1-95df-705681b3df0f/test.txt")
    irods.get_du("du-7370d7b5-ed0b-11e1-95df-705681b3df0f", "export")



if __name__ == "__main__":
    test_irods()
    
