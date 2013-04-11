'''
Globus Online based File Transfer
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
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
#sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from pilot.api import State
from bigjob import logger

from globusonline.transfer import api_client

#from globusonline.transfer.api_client.get_go_cookie import get_go_auth
from globusonline.transfer.api_client.goauth import get_access_token

class GlobusOnlineFileAdaptor(object):
    """ BigData Coordination File Management for Pilot Data """
    
    URL_PREFIX="go://"
    
    
    def __init__(self, service_url):        
        self.service_url = service_url
        result = urlparse.urlparse(service_url)
        self.host = result.netloc
        self.query = result.path      
        self.ep = self.__get_ep(self.query)
        self.path = self.__get_path(self.query)
        self.user = result.username  
        self.password = result.password
        
        #result = get_go_auth(ca_certs=None, username=self.user, password=self.password)
        result = get_access_token(ca_certs=None, username=self.user, password=self.password)
        
        #saml_cookie = result.cookie
        saml_cookie = result.token
        
        self.api = api_client.TransferAPIClient(username=self.user,
                                                goauth=saml_cookie
                                                )
        status_code, status_message, data = self.api.task_list()
        
        # initialize ssh client
        self.__state=State.New


    def get_security_context(self):
        """ Returns security context that needs to be available on the distributed
            node in order to access this Pilot Data """
        return None
    
        
    def initialize_pilotdata(self):
        # check whether directory exists
        try:
            self.api.endpoint_mkdir(self.ep, self.path)            
        except:
            pass
        self.__state=State.Running
        
        
    def get_pilotdata_size(self):
        # check size
        return None
    
    
    def delete_pilotdata(self):
        self.api.endpoint_delete(self.ep, self.path)
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
        self.api.endpoint_mkdir(self.ep, du_dir)
        
        
    def put_du(self, du):
        logging.debug("Copy DU using Globus Online")
        du_items = du.list()
        for i in du_items.keys():  
            local_filename=du_items[i]["local"]
            remote_path = os.path.join(self.path, str(du.id), os.path.basename(local_filename))
            logging.debug("Put file: %s to %s"%(local_filename, remote_path))                        
            if local_filename.startswith("ssh://"):
                # check if remote path is directory
                if self.__is_remote_directory(local_filename):
                    logging.warning("Path %s is a directory. Ignored."%local_filename)                
                    continue
                result = urlparse.urlparse(local_filename)
                source_host = result.netloc
                source_path = result.path
                logger.debug(str((source_host, source_path, self.host, remote_path)))
                if source_host == "" or source_host==None:
                    cmd = "scp "+ source_path + " " + self.host + ":" + remote_path
                else:
                    cmd = "scp "+ source_host+":"+source_path + " " + self.host + ":" + remote_path
                logger.debug("Command: %s"%cmd)
                os.system(cmd)                   
            elif(local_filename.startswith("go://")):
                self.__third_party_transfer_host(local_filename, self.service_url + "/" + str(du.id))

                
    
    def copy_du_to_url(self, du,  local_url, remote_url):
        base_dir = self.__get_path_for_du(du)
        logger.debug("copy_du_to_url, source: %s remote: %s"%(base_dir, remote_url))
        if remote_url.startswith("/") and os.path.exists(base_dir):
            target_path = remote_url
            source_path = base_dir
            logger.debug("Target and source host are localhost. Processing: %s" %(source_path))
            expanded_path = glob.glob(source_path + "/*")
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
    
    
    ###########################################################################
    # Pure File Management APIs
        
    def transfer(self, source_url, target_url):
        self.__third_party_transfer_host(source_url, target_url)    
    
    
    def create_remote_directory(self, target_url):
        if not self.__is_remote_directory(target_url):
            result = urlparse.urlparse(target_url)
            target_query = result.path
            target_ep = self.__get_ep(target_query)
            target_path = self.__get_path(target_query)
            result = self.api.endpoint_mkdir(target_ep, target_path)      
            logger.debug("GO EP: %s Directory: %s Creation Result: %s"%(target_ep, target_path, str(result)))      
            #task_id = result[2]["task_id"]
            #logger.debug("Transfer Request Result: %s Task ID: %s"%(str(result), task_id))
            #self.__wait_for_task(task_id)
            return True
        return True
    
    
    def get_path(self, target_url):
        result = urlparse.urlparse(target_url)
        target_query = result.path
        target_path = self.__get_path(target_query)
        return target_path
        
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
        try:
            result = urlparse.urlparse(url)
            target_query = result.path
            target_ep = self.__get_ep(target_query)
            target_path = self.__get_path(target_query)
            result = self.api.endpoint_ls(target_ep, target_path)      
            logger.debug("GO EP: %s Directory: %s Creation Result: %s"%(target_ep, target_path, str(result)))
            return True  
        except:
            pass       
        return False
                
        
    def __third_party_transfer_host(self, source_url, target_url):
        """
            Transfers from source URL to machine to target_url
        """
        transfer_start = time.time()
        result = urlparse.urlparse(source_url)
        source_query = result.path
        source_ep = self.__get_ep(source_query)
        source_path = self.__get_path(source_query)
        
        result = urlparse.urlparse(target_url)
        target_query = result.path
        target_ep = self.__get_ep(target_query)
        target_path = self.__get_path(target_query)


        target_path = os.path.join(target_path, os.path.basename(source_path))
        logger.debug("transfer from %s:%s to %s:%s"%(source_ep, source_path, target_ep, target_path))

        if os.path.exists(os.path.dirname(source_path)) and os.path.exists(target_path):
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
        
        transfer_id = self.api.submission_id()[2]["value"]    
        logger.debug("Transfer ID: %s"%transfer_id)    
        transfer = api_client.Transfer(transfer_id, source_ep, target_ep,
                                       deadline=None, sync_level=None, label=None)
        transfer.add_item(source_path=source_path, destination_path=target_path, recursive=False )        
        result = self.api.transfer(transfer)
        task_id = result[2]["task_id"]
        logger.debug("Transfer Request Result: %s Task ID: %s"%(str(result), task_id))
        self.__wait_for_task(task_id)
        logger.debug("Task ID: %s Time: %d sec"%(transfer_id, (time.time()-transfer_start)))
    
    
    def __get_ep(self, query_string):  
        if query_string.startswith("?"):
            query_string = query_string[1:]      
        comp = query_string.split("&")
        for i in comp:
            part = i.split("=")
            if part[0]=="ep":
                return part[1]                
    
    def __get_path(self, query_string):
        if query_string.startswith("?"):
            query_string = query_string[1:]      
        comp = query_string.split("&")
        for i in comp:
            part = i.split("=")
            if part[0]=="path":
                return part[1]      
    
    def __wait_for_task(self, task_id, timeout=None):
        status = "ACTIVE"
        while (timeout==None or timeout > 0) and status == "ACTIVE":
            code, reason, data = self.api.task(task_id, fields="status")
            status = data["status"]
            time.sleep(1)
            if timeout!=None:
                timeout -= 1

        if status != "ACTIVE":
            print "Task %s complete!" % task_id
            return True
        else:
            print "Task still not complete after %d seconds" % timeout
            return False
    
    
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
    
    
if __name__ == "__main__":
    go = GlobusOnlineFileAdaptor("http://drelu:bigjob@cli.globusonline.org?ep=drelu#egi&path=/ho")
    go.transfer("go://cli.globusonline.org?ep=drelu#MacBook&path=/~/cert.tar.gz", "go://cli.globusonline.org?ep=xsede#kraken&path=/~/")
    
