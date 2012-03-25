'''
SSH-based coordination scheme between manager and agent
'''
import paramiko
import urlparse
import pdb
import errno
import sys
import os
import stat
import logging
import traceback

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from pilot.api import State
from bigjob import logger


class SSHFileAdaptor(object):
    """ BigData Coordination File Management for Pilot Store """
    
    def __init__(self, service_url):        
        self.service_url = service_url
        result = urlparse.urlparse(service_url)
        self.host = result.netloc
        self.path = result.path        
        
        # initialize ssh client
        self.__client = paramiko.SSHClient()
        self.__client.load_system_host_keys()
        self.__client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.__client.connect(self.host)
        self.__sftp = self.__client.open_sftp()
        self.__state=State.New
                        
        
    def initialize_pilotdata(self):
        # check whether directory exists
        try:
            self.__sftp.chdir(self.path)            
        except IOError:
            # directory does not exist
            self.__sftp.mkdir(self.path)        
        self.__state=State.Running
        
        
    def get_pilotdata_size(self):
        # check size
        size = self.__sftp.stat(self.path).st_size
        return size
    
    
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
        try:
            self.__sftp.mkdir(du_dir)
        except:
            pass # dir already exists
        
        
    def put_du(self, du):
        self.put_du_scp(du)
                
                
    def put_du_paramiko(self, du):
        logging.debug("Copy DU using Paramiko")
        for i in du.list_data_unit_items():     
            remote_path = os.path.join(self.path, str(du.id), os.path.basename(i.local_url))
            logging.debug("Put file: %s to %s"%(i.local_url, remote_path))
                        
            if i.local_url.startswith("ssh://"):
                # check if remote path is directory
                if self.__is_remote_directory(i.local_url):
                    logging.warning("Path %s is a directory. Ignored."%i.local_url)                
                    continue      
                self.__third_party_transfer(i.local_url, remote_path)                
            else:
                if stat.S_ISDIR(os.stat(i.local_url).st_mode):
                    logging.warning("Path %s is a directory. Ignored."%i.local_url)                
                    continue            
                #self.__sftp.put(i.local_url, remote_path, self.put_progress, True)
                ssh_client, sftp_client = self.__create_sftp_client()
                sftp_client.put(i.local_url, remote_path)
                sftp_client.close()
                ssh_client.close()           


    def put_du_scp(self, du):
        logging.debug("Copy DU using SCP")
        for i in du.list_data_unit_items():     
            remote_path = os.path.join(self.path, str(du.id), os.path.basename(i.local_url))
            logging.debug("Put file: %s to %s"%(i.local_url, remote_path))                        
            if i.local_url.startswith("ssh://"):
                # check if remote path is directory
                if self.__is_remote_directory(i.local_url):
                    logging.warning("Path %s is a directory. Ignored."%i.local_url)                
                    continue
                
               
                #self.__third_party_transfer(i.local_url, remote_path)                
            else:
                if stat.S_ISDIR(os.stat(i.local_url).st_mode):
                    logging.warning("Path %s is a directory. Ignored."%i.local_url)                
                    continue         
            result = urlparse.urlparse(i.local_url)
            source_host = result.netloc
            source_path = result.path
            logger.debug(str((source_host, source_path, self.host, remote_path)))
            if source_host == "" or source_host==None:
                cmd = "scp "+ source_path + " " + self.host + ":" + remote_path
            else:
                cmd = "scp "+ source_host+":"+source_path + " " + self.host + ":" + remote_path
            logger.debug("Command: %s"%cmd)
            os.system(cmd)                   
                
    
    def copy_du_to_url(self, du,  local_url, remote_url):
        base_dir = self.__get_path_for_du(du)
        self.create_remote_directory(remote_url)  
        for filename in self.__sftp.listdir(base_dir):
            file_url = local_url + "/" + filename
            file_remote_url = remote_url + "/" + filename
            logger.debug("Copy " + file_url + " to " + file_remote_url)
            self.__third_party_transfer_host(file_url, file_remote_url)

        

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
    
        
    def put_progress(self, transfered_bytes, total_bytes):
        logging.debug("Bytes transfered %d/%d"%(transfered_bytes, total_bytes))
    
        
    
    ####################################################################################
    # pure file management methods
    # used by BJ file staging
    def transfer(self, source_url, target_url):
        self.__third_party_transfer_host(source_url, target_url)    
    
    def create_remote_directory(self, target_url):
        result = urlparse.urlparse(target_url)
        target_host = result.netloc
        target_path = result.path
        try:
            if not self.__is_remote_directory(target_url):
                client = paramiko.SSHClient()
                client.load_system_host_keys()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                client.connect(target_host)
                sftp = client.open_sftp()  
                sftp.mkdir(target_path)
                sftp.close()
                client.close()
                return True
        except:
            logger.error("Error creating directory: " + str(target_path) 
                         + " at: " + str(target_host))
            self.__print_traceback()
        return False
        
    def get_path(self, target_url):
        result = urlparse.urlparse(target_url)
        return result.path
        
    ###########################################################################
    # Private support methods
    def __get_path_for_du(self, du):
        return os.path.join(self.path, str(du.id))
    
    def __remove_directory(self, path):
        """Remove remote directory that may contain files.        
        """
        if self.__exists(path):
            for filename in self.__sftp.listdir(path):
                filepath = os.path.join(path, filename)
                logging.debug("Delete %s"%filepath)
                if stat.S_ISDIR(self.__sftp.stat(filepath).st_mode):
                    [self.__remove_directory(filepath)]
                else:
                    self.__sftp.remove(filepath)
            self.__sftp.rmdir(path)
            
  
            
        
    def __is_remote_directory(self, url):
        result = urlparse.urlparse(url)
        host = result.netloc
        path = result.path
        #if path.endswith("/"):
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(host)
        sftp = client.open_sftp()
        try:
            if stat.S_ISDIR(sftp.stat(path).st_mode):
                return True
            else:
                return False
        except:
            logger.error("Directory not found: %s"%path)
        sftp.close()
        client.close()
        return False
        
    def __create_sftp_client(self):
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(self.host)
        sftp_client = ssh_client.open_sftp()
        sftp_client.chdir(self.path)
        return ssh_client, sftp_client
        
    def __third_party_transfer_host(self, source_url, target_url):
        """
            Transfers from source URL to machine of PS (target path)
        """
        result = urlparse.urlparse(source_url)
        source_host = result.netloc
        source_path = result.path
        
        result = urlparse.urlparse(target_url)
        target_host = result.netloc
        target_path = result.path
          
        python_script= """import sys
import os
import urllib
import sys
import time
import paramiko

client = paramiko.SSHClient()
client.load_system_host_keys()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect("%s")
sftp = client.open_sftp()
sftp.put("%s", "%s")
"""%(target_host, source_path, target_path)

        logging.debug("Execute: \n%s"%python_script)
        source_client = paramiko.SSHClient()
        source_client.load_system_host_keys()
        source_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        source_client.connect(source_host)
        stdin, stdout, stderr = source_client.exec_command("python -c \'%s\'"%python_script)
        stdin.close()
        logging.debug("************************************************")
        logging.debug("Stdout: %s\nStderr:%s", stdout.read(), stderr.read())
        logging.debug("************************************************")
        
    
    def __third_party_transfer(self, source_url, target_path):
        """
            Transfers from source URL to machine of PS (target path)
        """
        result = urlparse.urlparse(source_url)
        source_host = result.netloc
        source_path = result.path
        
        python_script= """import sys
import os
import urllib
import sys
import time
import paramiko

client = paramiko.SSHClient()
client.load_system_host_keys()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect("%s")
sftp = client.open_sftp()
sftp.get("%s", "%s")

"""%(source_host, source_path, target_path)

        logging.debug("Execute: \n%s"%python_script)
        source_client = paramiko.SSHClient()
        source_client.load_system_host_keys()
        source_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        source_client.connect(self.host)
        stdin, stdout, stderr = source_client.exec_command("python -c \'%s\'"%python_script)
        stdin.close()
        logging.debug("************************************************")
        logging.debug("Stdout: %s\nStderr:%s", stdout.read(), stderr.read())
        logging.debug("************************************************")
    
    def __exists(self, path):
        """Return True if the remote path exists
        """
        try:
            self.__sftp.stat(path)
        except IOError, e:
            if e.errno == errno.ENOENT:
                return False
            raise
        else:
            return True
   
   
    def __print_traceback(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "*** print_tb:"
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        print "*** print_exception:"
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)
    