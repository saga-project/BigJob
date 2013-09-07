from pilot import *
from bigjob import logger
from redis.client import Lock

try:
    import json
except ImportError:
    import simplejson as json

from saga import Url as SAGAUrl

class RedisCoordinationAdaptor:
    """
    	Dummy Adaptor - No distributed coordination done    
    """
    BASE_URL="redis://localhost/"
    SEPARATOR=":"
    
    PILOT_PATH="pilot"
    PILOT_DATA_PATH=PILOT_PATH
    PILOT_DATA_SERVICE_PATH=PILOT_DATA_PATH + SEPARATOR + "pds"
    DATA_UNIT_SERVICE_PATH=PILOT_DATA_PATH + SEPARATOR +"dus"
    COMPUTE_DATA_SERVICE_PATH = PILOT_DATA_PATH + SEPARATOR + "cds"

    ###########################################################################
    # Construct a base url for an application
     
    @classmethod
    def configure_base_url(cls, base_url):
        cls.BASE_URL=base_url
    
    @classmethod
    def get_base_url(cls, application_id):
        if cls.BASE_URL==None:
            logger.error("Coordination URL not set. Exiting Pilot-Data.")
            raise Exception("Coordination URL not set. Exiting Pilot-Data.")
        surl = SAGAUrl(cls.BASE_URL)
        base_url = surl.scheme + "://" + surl.host + "/" + application_id 
        logger.debug(base_url)
        return base_url
    
    ###########################################################################
    # Pilot Store Service related methods
    
    @classmethod  
    def add_pds(cls, application_url, pds):
        pds_url_no_dbtype = cls.get_pds_url(application_url, pds.id)
        pds_url = cls.__get_url(pds_url_no_dbtype)
        logger.debug("Create PDS directory at %s"%pds_url)
        return pds_url_no_dbtype
    
    
    @classmethod
    def delete_pds(cls, pds_url):
        pds_url = cls.__get_url(pds_url)
        
        #pds_dir.remove(pds_url, saga.name_space.Recursive)  
    
    ###########################################################################
    # Pilot Data related methods
    
    @classmethod
    def add_pd(cls, pds_url, pd):
        pd_url =pds_url+  RedisCoordinationAdaptor.SEPARATOR + pd.id
        return pd_url
    
    
    @classmethod
    def update_pd(cls, pd):
        du_urls=None
        if len(pd.data_unit_urls) > 0:
            du_urls = pd.data_unit_urls
        
        pd_dict={
                 "data_unit_urls": du_urls,
                 "pilot_data": pd.to_dict(),
                 "pilot_data_description": pd.pilot_data_description,
                 "security_context": pd.security_context
                 }
        
        cls.__store_entry(pd.url+RedisCoordinationAdaptor.SEPARATOR + "info", pd_dict)
    
        
    @classmethod
    def get_pd(cls, pd_url):
        logger.debug("GET PD: " + pd_url)     
        pd_dict=cls.__retrieve_entry(pd_url + RedisCoordinationAdaptor.SEPARATOR + "info")        
        return pd_dict
        
    
    @classmethod
    def list_pd(cls, pds_url):
        """ return a list of urls to pd managed by the PDS """
        pds_url = cls.__get_url(pds_url)
        logger.debug("List PD at %s"%pds_url)
        
    
    @classmethod
    def delete_pd(cls, pds_url):
        pds_url = cls.__get_url(pds_url)
        #pd_dir = saga.advert.directory(saga.url(pds_url), 
        #                                saga.advert.Create | 
        #                                saga.advert.CreateParents | 
        #                                saga.advert.ReadWrite)
        #pd_dir.remove(pds_url, saga.name_space.Recursive)  
    
        
    ###########################################################################
    # Compute Data Service related methods
    @classmethod  
    def add_cds(cls, application_url, cds):
        cds_url_no_dbtype = cls.get_cds_url(application_url, cds.id)
        cds_url = cls.__get_url(cds_url_no_dbtype)
        logger.debug("Create CDS directory at %s"%cds_url)
        #saga.advert.directory(cds_url, saga.advert.Create | 
        #                               saga.advert.CreateParents | 
        #                               saga.advert.ReadWrite)
        return cds_url_no_dbtype
    
    
    @classmethod  
    def update_cds(cls, cds_url, cds):
        
        # Storage and Compute Resources
        pds_urls = [i.url for i in cds.pilot_data_services]
        #cls.__store_entry(cls.__remove_dbtype(cds_url)+"/pds/", pds_urls)
        
        pjs_urls = [i.url for i in cds.pilot_job_services]
        #cls.__store_entry(cls.__remove_dbtype(cds_url)+"/cds/", pjs_urls)
        
        # currently managed PDs and WUs
        pd_urls = [i.url for i in cds.data_units.values()]
        #cls.__store_entry(cls.__remove_dbtype(cds_url)+"/du/", pd_urls)
        
        wu_urls = [i.url for i in cds.compute_units.values()]
        #cls.__store_entry(cls.__remove_dbtype(cds_url)+"/cu/", wu_urls)
            
        
    @classmethod
    def delete_cds(cls, cds_url):
        cds_url = cls.__get_url(cds_url)
        #cds_dir = saga.advert.directory(saga.url(cds_url), 
        #                                saga.advert.Create | 
        #                                saga.advert.CreateParents | 
        #                                saga.advert.ReadWrite)
        # cds_dir.remove(cds_url, saga.name_space.Recursive)
    
    
    
        
    ###########################################################################
    #  Data Unit related methods
    @classmethod
    def add_du(cls, root_url, du):
        du_url = root_url  +  RedisCoordinationAdaptor.SEPARATOR + du.id     
        du_url = cls.__get_url(du_url)
        return du_url

    
    @classmethod
    def get_du(cls, du_url):
        logger.debug("**** GET DU: " + str(du_url))
        du_dict=cls.__retrieve_entry(du_url+ RedisCoordinationAdaptor.SEPARATOR + "info")   
        logger.debug("Retrieved DU: " + du_url + " Content: " + str(du_dict))     
        return du_dict
    
     
     
    @classmethod  
    def update_du_state(cls, du, state):
        logger.debug("**** Update data unit STATE at: " + du.url + " to: " + str(state))
        cls.__store_entry_item(du.url + RedisCoordinationAdaptor.SEPARATOR + "info", "state", state)
 
     
    @classmethod  
    def update_du(cls, du):
        logger.debug("**** Update data unit FULL at: " + du.url)
        du_dict_list = [i.to_dict() for i in du.data_unit_items]
        pd_urls=[]
        if du.pilot_data!=None: pd_urls = [i.url for i in du.pilot_data]
        du_dict = {
                "data_unit_description":du.data_unit_description,
                "state": du.state,
                "pilot_data": pd_urls,
                "data_unit_items": du_dict_list
                }
        logger.debug("Update DU: " + str(du.url) + " to " + str(du_dict["state"]))
        cls.__store_entry(du.url + RedisCoordinationAdaptor.SEPARATOR + "info", du_dict)
        
       
    @classmethod
    def list_du(cls, pd_url):
        """ return a list of urls to du managed by the PDS """
        pd_url = cls.__get_url(pd_url)
        logger.debug("List Data-Units of Pilot-Data at %s"%pd_url)
        dus = cls.__list_keys(pd_url+":du-*")
        return dus
    
    
    @classmethod
    def delete_du(cls, du_url):
        du_url = cls.__get_url(du_url)
        #du_dir = saga.advert.directory(saga.url(du_url), 
        #                                saga.advert.Create | 
        #                                saga.advert.CreateParents | 
        #                                saga.advert.ReadWrite)
        #du_dir.remove(du_url, saga.name_space.Recursive)  
    
    
    
    ###########################################################################
    # URL Tweaking
    @classmethod
    def get_pds_url(cls, application_url, pds_id):
        pds_url = application_url +  RedisCoordinationAdaptor.SEPARATOR +pds_id        
        logger.debug("PDS URL: %s"%(pds_url))
        return pds_url
    
    
    @classmethod
    def get_cds_url(cls, application_url, cds_id):
        cds_url = application_url +  RedisCoordinationAdaptor.SEPARATOR +cds_id        
        logger.debug("CDS URL: %s"%(cds_url))
        return cds_url
    
    
    ###########################################################################
    # internal Redis-related methods
    @classmethod
    def __get_redis_api_client(cls):
        import redis
        ''' Initialize Redis API Client     '''
        saga_url = SAGAUrl(RedisCoordinationAdaptor.BASE_URL)
        username = saga_url.username
        server = saga_url.host
        server_port = saga_url.port
        if username==None or username=="":
            redis_client = redis.Redis(host=server, port=server_port, db=0)
        else:
            redis_client = redis.Redis(host=server, port=server_port, password=username, db=0)
        
        try:
            redis_client.ping()
        except:
            logger.error("Please start Redis server!")
            raise Exception("Please start Redis server!")
        return redis_client
    
    
    @classmethod
    def __get_url(cls, url):
        return url
    
    
    @classmethod
    def __list_keys(cls, search_url):
        redis_client = cls.__get_redis_api_client()
        keys = redis_client.keys(search_url)
        keys_normalized = [i[:i.index(":info")] for i in keys]
        return keys_normalized
        
    @classmethod
    def __store_entry_item(cls, entry_url, item_key, item_value):
        entry_url = cls.__get_url(entry_url)
        redis_client = cls.__get_redis_api_client()
        lock_name=entry_url+":lock"
        logger.debug("Acquire Redis lock for update: " + lock_name)
        lock = Lock(redis_client, lock_name, timeout=None, sleep=0.1)
        acquired=lock.acquire(blocking=True)
        logger.debug("Lock acquired: " + str(acquired))
        redis_client.hset(entry_url, item_key, item_value)
        lock.release()
        logger.debug("Stored Redis entry at: " + entry_url 
                      + " Key: " + str(json.dumps(item_key))
                      + " Value: " + str(json.dumps(item_value))
                      )
    
        
    @classmethod
    def __store_entry(cls, entry_url, content):
        entry_url = cls.__get_url(entry_url)
        redis_client = cls.__get_redis_api_client()
        lock_name=entry_url+":lock"
        logger.debug("Acquire Redis lock for update: " + lock_name)
        lock = Lock(redis_client, lock_name, timeout=None, sleep=0.1)
        acquired=lock.acquire(blocking=True)
        logger.debug("Lock acquired: " + str(acquired))
        redis_client.hmset(entry_url, content)
        lock.release()
        logger.debug("Stored Redis entry at: " + entry_url 
                      + " Content: " + str(json.dumps(content)))
        
    @classmethod
    def __retrieve_entry(cls, entry_url):
        entry_url = cls.__get_url(entry_url)
        redis_client = cls.__get_redis_api_client()
        content = redis_client.hgetall(entry_url)
        
        logger.debug("Retrieve Redis entry at: " + entry_url 
                      + " Content: " + str(json.dumps(content)))
        return content
