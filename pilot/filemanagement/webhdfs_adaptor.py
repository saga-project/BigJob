import sys, os
import stat
import urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/../../../webhdfs-py/")

from pilot.api import State
from bigjob import logger
logger.debug(str(sys.path))

from webhdfs.webhdfs import WebHDFS 


class WebHDFSFileAdaptor(object):
    
    HDFS_USER_NAME="luckow"
    HDFS_SERVICE_HOST="192.168.2.108"
    HDFS_SERVICE_PORT=50070     
    
    def __init__(self, service_url):     
        
        self.service_url = service_url
        
        try:
            result = urlparse.urlparse(service_url)
            self.host = result.netloc
            self.path = result.path        
        except:
            logger.error("Error parsing URL.")
            
        self.__state=State.New
        self.__webhdfs= WebHDFS(self.HDFS_SERVICE_HOST, 
                               self.HDFS_SERVICE_PORT,
                               self.HDFS_USER_NAME)
        
    def get_security_context(self):
        """ Returns security context that needs to be available on the distributed
            node in order to access this Pilot Data """
        return None
        
        
    def initialize_pilotstore(self):
        self.__webhdfs.mkdir(self.path)
        
        
    def get_pilotstore_size(self):
        return 0
    
    
    def delete_pilotstore(self):
        self.__webhdfs.rmdir(self.path)
        
    def get_state(self):
        return self.__state
            
            
    def create_pd(self, pd_id):
        pd_dir = self.__get_pd_path(pd_id)
        logger.debug("mkdir: " + pd_dir) 
        self.__webhdfs.mkdir(pd_dir)
        
        
    def put_pd(self, pd):
        for i in pd.list_data_units():     
            remote_path = os.path.join(self.__get_pd_path(pd.id), os.path.basename(i.local_url))
            logger.debug("Put file: %s to %s"%(i.local_url, remote_path))
                        
            if i.local_url.startswith("file://") or i.local_url.startswith("/"):
                if stat.S_ISDIR(os.stat(i.local_url).st_mode):
                    logger.warning("Path %s is a directory. Ignored."%i.local_url)                
                    continue            
                self.__webhdfs.copyFromLocal(i.local_url, remote_path)
            else:
                logger.error("File URLs: %s not supported"%i.local_url)
                             

    def copy_pd_to_url(self, pd,  local_url, remote_url):
        
        if not remote_url.startswith("file://") and not remote_url.startswith("/"):
            logger.error("Only local URLs supported")
            return
        
        result = urlparse.urlparse(remote_url)
        path = result.path    
        # create directory
        try:
            os.makedirs(path)
        except:
            logger.debug("Directory: %s already exists."%path)
            
        base_dir = self.__get_pd_path(pd.id)
        for filename in self.__webhdfs.listdir(base_dir):
            file_url = local_url + "/" + filename
            file_remote_url = remote_url + "/" + filename
            logger.debug("GET " + file_url + " to " + file_remote_url)
            self.__webhdfs.copyToLocal(file_url, file_remote_url)



    def copy_pd(self, pd, ps_new):
        pass
    

    def get_pd(self, pd, target_url):
        remote_url = target_url
        local_url =  self.__get_pd_path(pd.id)
        self.copy_pd_to_url(pd, local_url, remote_url)         
    
        
    def remove_pd(self, pd):
        self.__webhdfs.rmdir(self.__get_pd_path(pd.id))
    
    
    ###########################################################################
    # Internal methods
    def __get_pd_path(self, pd_id):
        return os.path.join(self.path, str(pd_id))
    