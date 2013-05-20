'''
iRods based File Transfer Implementation
'''
import urlparse
import datetime
import errno
import sys
import os
import stat
import logging
import traceback
import time
import re
import shutil
import pdb
import glob
import pexpect

# This is for local debugging!
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import saga
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
            logger.debug("Found: " + env_var + " in URL.")
            logger.debug("Env list: " + str(os.environ))
            if os.environ.has_key(env_var):
                self.localpath = re.sub(r'\$\{.*\}', os.environ[env_var], self.localpath)
                #self.localpath = os.environ[env_var]
                logger.debug("Expanding URL Path to: " + self.localpath)
                return True
        logger.debug("No expansion in: " + self.localpath)
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
        start = time.time()
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
        logger.debug("Finished Put DU in: " + str(time.time()-start) + " sec.")
                
    
    def get_du(self, du, target_url):
        #du_id = "du-7370d7b5-ed0b-11e1-95df-705681b3df0f"
        start = time.time()
        du_id = du.id
        logger.debug("Get DU: " + str(du_id))
        if self.is_local:
            command = "cp -r %s %s"%(os.path.join(self.localpath, du_id), target_url)
            source_path = os.path.join(self.localpath, du_id, "*")
            target_path = target_url
            logger.debug("Target and source host are localhost. Processing: %s" %(source_path))
            expanded_path = glob.glob(source_path)
            logger.debug("Expanded path: " + str(expanded_path))
            for path in expanded_path:
                if os.path.isdir(path):
                    logger.debug("Source path %s is directory"%path)
                    files = os.listdir(path)
                    for i in files:
                        try:
                            os.symlink(os.path.join(files, i), target_path)
                            os.chmod(os.path.join(target_path, os.path.basename(path)), 0777)
                        except:
                            self.__print_traceback()
                else:
                    try:
                        os.symlink(path, os.path.join(target_path, os.path.basename(path)))
                        os.chmod(os.path.join(target_path, os.path.basename(path)), 0777)
                    except:
                        self.__print_traceback()

        else:
            command = "iget -f -r %s %s"%(du_id, target_url)
            logger.debug(command)
            self.__run_command(command)
            
            full_path = os.path.join(target_url, du_id)
            #logger.debug("Path: " + str(full_path) + " Exists: " + str(os.path.exists(full_path)))
            #while os.path.exists(full_path)==False:
            #    time.sleep(1)
    
            for i in os.listdir(full_path):
                try:
                    logger.debug("chmod " + str(i))
                    os.chmod(os.path.join(full_path, i), 0777)
                    logger.debug("move " + str(i))
                    shutil.move(os.path.join(full_path, i), target_url)
                except:
                    self.__print_traceback()
    
            shutil.rmtree(full_path, ignore_errors=True)
            #time.sleep(2)
            #if target_url==".":
            #    target_url = os.getcwd()
            #command = "mv %s/* %s"%(os.path.join(target_url, du_id), target_url)
            #self.__run_command(command)
            logger.debug("Finished Get DU " + du.id + " in: " + str(time.time()-start) + " sec.")
        
   
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
        start = time.time()
        if self.is_local:
            command = "cp -r %s %s"%(source, target)
        else:
            command = "iput -f -R %s %s %s"%(self.resource_group, source, target)
        self.__run_command(command)
        put_time = time.time() - start
        number_replica = 0
        if self.is_local==False:
            #pdb.set_trace()
            home_directory= self.__run_command("ipwd")[0].strip()
            full_filename = os.path.join(home_directory, target)
            command = "irepl-osg -f %s -G %s"%(full_filename, self.resource_group)
            output = self.__run_command(command) 
            for i in output:
                if i.find("copied") > 0 or i.find("replica")>0:
                    number_replica = number_replica + 1 
        rep_time = time.time() - start - put_time
        logger.info("Upload;Replication;Total;File Size;Backend;Number Replica;Timestamp: %f;%f;%f;%d;%s;%d;%s"%(put_time, rep_time, time.time()-start, os.path.getsize(source), self.resource_group, number_replica, datetime.datetime.today().isoformat()))
         

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
        traceback.print_tb(exc_traceback, limit=1, file=sys.stderr)
        print "*** print_exception:"
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stderr)
    
    
def test_irods():
    irods = iRodsFileAdaptor("irods://gw68/${OSG_DATA}/osg/home/luckow/?vo=osg&resource-group=osgGridFtpGroup")
    irods.initialize_pilotdata()
    irods.create_du("du-7370d7b5-ed0b-11e1-95df-705681b3df0f")
    irods._put_file("test.txt", "du-7370d7b5-ed0b-11e1-95df-705681b3df0f/test.txt")
    irods.get_du("du-7370d7b5-ed0b-11e1-95df-705681b3df0f", "export")



if __name__ == "__main__":
    test_irods()
    
