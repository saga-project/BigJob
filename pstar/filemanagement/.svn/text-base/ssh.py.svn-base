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
from bigdata.troy.compute.api import State
from bigdata import logger

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
                        
        
    def initialize_pilotstore(self):
        # check whether directory exists
        try:
            self.__sftp.chdir(self.path)            
        except IOError:
            # directory does not exist
            self.__sftp.mkdir(self.path)        
        self.__state=State.Running
        
        
    def get_pilotstore_size(self):
        # check size
        size = self.__sftp.stat(self.path).st_size
        return size
    
    
    def delete_pilotstore(self):
        self.__remove_directory(self.path)
        self.__state=State.Done
    
        
    def get_state(self):
        if self.__client.get_transport().is_active()==True:
            return self.__state
        else:
            self.__state=State.Failed
            return self.__state            
            
    def create_pd(self, pd_id):
        pd_dir = os.path.join(self.path, str(pd_id))
        logger.debug("mkdir: " + pd_dir)
        try:
            self.__sftp.mkdir(pd_dir)
        except:
            pass # dir already exists
        
        
    def put_pd(self, pd):
        for i in pd.list_data_units():     
            remote_path = os.path.join(self.path, str(pd.id), os.path.basename(i.local_url))
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
                self.__sftp.put(i.local_url, remote_path, self.put_progress, True)
                             

    def copy_pd_to_url(self, pd,  local_url, remote_url):
        base_dir = self.__get_path_for_pd(pd)
        self.__create_remote_directory(remote_url)  
        for filename in self.__sftp.listdir(base_dir):
            file_url = local_url + "/" + filename
            file_remote_url = remote_url + "/" + filename
            logger.debug("Copy " + file_url + " to " + file_remote_url)
            self.__third_party_transfer_host(file_url, file_remote_url)


    def copy_pd(self, pd, ps_new):
        remote_url = ps_new.service_url + "/" + str(pd.id)
        local_url =  self.service_url  + "/" + str(pd.id)
        self.copy_pd_to_url(pd, local_url, remote_url)  
        
    
    def get_pd(self, pd, target_url):
        
        #result = urlparse.urlparse(target_url)
        #target_host = result.netloc
        #target_path = result.path
        
        #base_dir = self.__get_path_for_pd(pd)
        #logging.debug("Copy PD from %s to %s"%(base_dir, target_url))        
        #if not os.path.exists(target_url):
        #    os.makedirs(target_url)
        remote_url = target_url
        local_url =  self.service_url  + "/" + str(pd.id)
        self.copy_pd_to_url(pd, local_url, remote_url)  
        
        #for filename in self.__sftp.listdir(base_dir):
            #logging.debug("Copy %s"%filename)
            #self.__sftp.get(os.path.join(base_dir, filename), os.path.join(target_url, filename))
    
        
    def remove_pd(self, pd):
        self.__remove_directory(os.path.join(self.path, pd.id))
    
        
    def put_progress(self, transfered_bytes, total_bytes):
        logging.debug("Bytes transfered %d/%d"%(transfered_bytes, total_bytes))
    
        
    
    ###########################################################################
    # Private support methods
    def __get_path_for_pd(self, pd):
        return os.path.join(self.path, str(pd.id))
    
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
            
    def __create_remote_directory(self, target_url):
        result = urlparse.urlparse(target_url)
        target_host = result.netloc
        target_path = result.path
        try:
            client = paramiko.SSHClient()
            client.load_system_host_keys()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(target_host)
            sftp = client.open_sftp()            
            #s = sftp.stat(os.path.dirname(target_path))
            #if stat.S_ISDIR(s.st_mode):
            #    logger.debug("Directory: " + target_path + " exists.")
            #else:
            sftp.mkdir(target_path)
            sftp.close()
            client.close()
        except:
            logger.error("Error creating directory: " + str(target_path) 
                         + " at: " + str(target_host))
            self.__print_traceback()
            
        
    def __is_remote_directory(self, url):
        result = urlparse.urlparse(url)
        host = result.netloc
        path = result.path
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(host)
        sftp = client.open_sftp()
        if stat.S_ISDIR(sftp.stat(path).st_mode):
            return True
        else:
            return False
        sftp.close()
        client.close()
        
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
    