'''
SSH-based coordination scheme between manager and agent
'''
import urlparse
import pdb
import glob
import errno
import sys
import os
import stat
import logging
import traceback
import pexpect
from pexpect import TIMEOUT
from pilot.api.api import PilotError

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from pilot.api import State
from bigjob import logger

SSH_OPTS="-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o NumberOfPasswordPrompts=0"


class SSHFileAdaptor(object):
    """ BigData Coordination File Management for Pilot Store """
    
    def __init__(self, service_url, security_context=None, pilot_data_description=None):        
        self.service_url = service_url
        result = urlparse.urlparse(service_url)
        self.host = result.hostname
        self.path = result.path        
        self.user = result.username
        
        self.pilot_data_description=pilot_data_description
        
        # handle security context
        self.userkey=None
        self.security_context=security_context
         
        # try to recover key from pilot_data_description
        if self.pilot_data_description!=None and\
           self.pilot_data_description.has_key("userkey"):
            self.userkey=self.pilot_data_description["userkey"]

        logger.debug("Security Context: " + str(self.security_context))
            
        # try to recover key from security context
        if security_context!=None and security_context!="None":
            logger.debug("Attempt to restore SSH credentials from security context: " + str(security_context))
            security_context = eval(security_context)
            key=security_context["userkey"]             
            self.userkey=os.path.join(os.getcwd(), ".ssh/id_rsa")
            if os.path.exists(os.path.join(os.getcwd(),".ssh"))==False:
                os.makedirs(os.path.join(os.getcwd(), ".ssh"))
            logger.debug("Write key: " + str(type(key)) + " to: " + self.userkey)
            try:
                f = open(self.userkey, "w") 
                for i in key:
                    logger.debug("write: " + str(i))
                    f.write(i)
                f.close()
                os.chmod(self.userkey, 0600)
            except:
                self.__print_traceback()
        
        if self.__run_check()==False:
            sys.stderr.write("Unable to connect/initialize Pilot-Data. Exit BigJob.")
            raise PilotError("Unable to connect/initialize Pilot-Data. Exit BigJob.")
        
        

    def get_security_context(self):
        """ Returns security context that needs to be available on the distributed
            node in order to access this Pilot Data """
        if (self.security_context==None or self.security_context=="None") and self.pilot_data_description.has_key("userkey"):
            f = open(self.pilot_data_description["userkey"])
            key = f.readlines()
            f.close
            self.security_context = {"userkey":key}
        logger.debug("Return security context: " + str(self.security_context))        
        return self.security_context
        
        
    def initialize_pilotdata(self):
        # check whether directory exists
        try:
            command = "mkdir -p %s"%self.path 
            self.__run_ssh_command(self.userkey, self.user, self.host, command)      
        except IOError:
            self.__print_traceback()
            # directory does not exist
        self.__state=State.Running
        
        
    def get_pilotdata_size(self):
        return None
    
    
    def delete_pilotdata(self):
        self.__remove_directory(self.path)
        self.__state=State.Done
    
        
    def get_state(self):
        if self.__client.get_transport().is_active()==True:
            return self.__state
        else:
            self.__state=State.Failed
            return self.__state            
            
    def create_du(self, du_id):
        du_dir = os.path.join(self.path, str(du_id))
        logger.debug("mkdir: " + du_dir)
        command = "mkdir %s"%du_dir
        self.__run_ssh_command(self.userkey, self.user, self.host, command)
        
        
    def put_du(self, du):
        self.put_du_scp(du)
                

    def put_du_scp(self, du):
        logger.debug("Copy DU using SCP")
        du_items = du.list()
        for i in du_items.keys():     
            local_filename = du_items[i]["local"]
            remote_path = os.path.join(self.path, str(du.id), os.path.basename(local_filename))
            logger.debug("Put file: %s to %s"%(i, remote_path))                        
            if local_filename.startswith("ssh://"):
                # check if remote path is directory
                if self.__is_remote_directory(local_filename):
                    logger.warning("Path %s is a directory. Ignored."%local_filename)                
                    continue
               
                #self.__third_party_transfer(i.local_url, remote_path)                
            else:
                try:
                    if stat.S_ISDIR(os.stat(local_filename).st_mode):
                        logger.warning("Path %s is a directory. Ignored."%local_filename)                
                        continue
                except:
                    pass         
            result = urlparse.urlparse(local_filename)
            source_host = result.netloc
            source_path = result.path
            source_user = result.username
            logger.debug(str((source_host, source_path, self.host, remote_path)))
            self.__run_scp_command(self.userkey, source_user, source_host, source_path, self.user, self.host, remote_path)        
    
  
    def copy_du(self, du, pd_new):
        remote_url = pd_new.service_url + "/" + str(du.id)
        local_url =  self.service_url  + "/" + str(du.id)
        self.copy_du_to_url(du, local_url, remote_url)  
        
    
    def get_du(self, du, target_url):
        remote_url = target_url
        local_url =  self.service_url  + "/" + str(du.id)
        logger.debug("get_du(): copy %s to %s:"%(local_url, remote_url))
        self.copy_du_to_url(du, local_url, remote_url)  
        
        
    def remove_du(self, du):
        self.__remove_directory(os.path.join(self.path, du.id))
    
        
    def put_progress(self, transfered_bytes, total_bytes):
        logger.debug("Bytes transfered %d/%d"%(transfered_bytes, total_bytes))
    
        
    
    ####################################################################################
    # pure file management methods
    # used by BJ file staging
    def transfer(self, source_url, target_url):
        self.__third_party_transfer_scp(source_url, target_url)    
    
    
    def create_remote_directory(self, target_url):
        result = urlparse.urlparse(target_url)
        target_host = result.hostname
        target_path = result.path
        target_user = result.username
        logger.debug("Create directory: %s"%target_path)
        command = "mkdir %s"%target_path
        rc = self.__run_ssh_command(self.userkey, target_user, target_host, command)
        if rc==0:
            return True
        else:
            return False
                
        
    def get_path(self, target_url):
        result = urlparse.urlparse(target_url)
        return result.path
    
        
    def copy_du_to_url(self, du,  local_url, remote_url):
        self.create_remote_directory(remote_url)
        self.__third_party_transfer_scp(local_url + "/*", remote_url)
  
            
    ###########################################################################
    # Private support methods
    def __get_path_for_du(self, du):
        return os.path.join(self.path, str(du.id))
    
    
    def __remove_directory(self, path):
        """Remove remote directory that may contain files.        
        """
        if self.__exists(path):
            command = "rm -rf %s"%path
            rc = self.__run_ssh_command(self.userkey, self.user, self.host, command)
            if rc==0:
                return True
            else:
                return False
            
        
    def __is_remote_directory(self, url):
        result = urlparse.urlparse(url)
        host = result.hostname
        path = result.path
        user = result.username        
        command = "test -d %s"%path
        rc = self.__run_ssh_command(self.userkey, user, host, command)
        if rc==0:
            logger.debug("Directory found: %s"%path)
            return True
        else:
            logger.debug("Directory not found: %s"%path)
            return False
            

    def __third_party_transfer_scp(self, source_url, target_url):
        result = urlparse.urlparse(source_url)
        source_host = result.hostname
        source_path = result.path
        source_user = result.username
        if source_host==None or source_host=="":
            source_host="localhost"

        result = urlparse.urlparse(target_url)
        target_host = result.hostname
        target_path = result.path
        target_user = result.username
        if target_host==None or target_host=="":
            target_host="localhost"

        #check whether this is a local transfer
        if os.path.exists(os.path.dirname(source_path)):
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
                        except:
                            self.__print_traceback()
                else:
                    try:
                        os.symlink(path, os.path.join(target_path, os.path.basename(path)))
                    except:
                        self.__print_traceback()
        else:
            self.__run_scp_command(self.userkey, source_user, source_host, source_path, target_user, target_host, target_path)


 
    def __exists(self, path):
        """Return True if the remote path exists
        """
        command = "test -e %s"%path
        rc = self.__run_ssh_command(self.userkey, self.user, self.host, command)
        if rc==0:
            return True
        else:
            return False
        
 
    
    def __run_ssh_command(self, userkey, user, host, command):
        output = self.__run_ssh_command_rc(userkey, user, host, command)[0]
        return output
         
    
    def __run_ssh_command_rc(self, userkey, user, host, command):
        prefix=""
        if host != None:
            prefix = "ssh " + SSH_OPTS + " "
            if userkey != None:
                prefix = prefix + " -i " + userkey + " "
            if user!=None:
                prefix = prefix + " " + user+ "@" 
            prefix = prefix + host
        
        command = prefix + " " + command
        logger.debug(command.strip())
        child = pexpect.spawn(command.strip(), timeout=None)
        output = child.readlines()
        logger.debug("Run %s Output: %s"%(command, str(output)))
        child.close()
        return (output, child.exitstatus) 


    def __run_scp_command(self, userkey, source_user, source_host, source_path, target_user, target_host, target_path):
        logger.debug("Create scp command: source_user: %s, source_host: %s"%(source_user, source_host))
        command = "scp " + SSH_OPTS + " "
        if userkey != None:
            command = command + "-i " + userkey + " "
        
        if source_host != None and source_host!="" and source_host!="localhost":
            if source_user!=None:
                command = command + " " + source_user + "@" 
            if source_host!="localhost":
                command = command + source_host + ":"
        
        # path is a must parameter
        command = command + source_path + " "
        
        logger.debug("Create scp command: target_user: %s, target_host: %s"%(str(target_user), str(target_host)))
        if target_host != None and target_host!="" and target_host!="localhost":
            if target_user!=None:
                command = command + " " + target_user + "@" 
            if target_host!="localhost":
                command = command + target_host + ":"
            
        command = command + target_path 
        logger.debug(command)    
        child = pexpect.spawn(command.strip(), timeout=None)
        password_error=False
        try:
            child.timeout=300
            child.expect("password:",timeout=300, searchwindowsize=5024)
            password_error=True
        except Exception as ex:
            logger.debug("No password prompt error found" + str(ex))

        if password_error:
            raise PilotError("SSH key-less login not correctly setup.")
        output = child.readlines()
        logger.debug("Run %s Output: %s"%(command, str(output)))
        child.close()
        return child.exitstatus
        
    def __run_check(self):
        rc = self.__run_ssh_command_rc(self.userkey, self.user, self.host, "/bin/date")[1]
        if rc == 0:
            return True
        else:
            return False 
   
    def __print_traceback(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "*** print_tb:"
        traceback.print_tb(exc_traceback, limit=1, file=sys.stderr)
        print "*** print_exception:"
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stderr)
    
