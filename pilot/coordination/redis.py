import logging
import json
import pdb

from pilot import *
from bigjob import logger
from bliss.saga import Url as SAGAUrl

class RedisCoordinationAdaptor:
    """
    	Redis Adaptor 
    """
    BASE_URL="redis://localhost:6379/"    
    PILOT_PATH="pilot"
    PILOT_DATA_PATH=PILOT_PATH
    PILOT_DATA_SERVICE_PATH=PILOT_DATA_PATH+"/pds"
    DATA_UNIT_SERVICE_PATH=PILOT_DATA_PATH+"/dus"
    COMPUTE_DATA_SERVICE_PATH = PILOT_DATA_PATH + "/cds"

    
    ###########################################################################
    # Construct a base url for an application
    
    @classmethod
    def get_base_url(cls, application_id):
        surl = SAGAUrl(cls.BASE_URL)
        base_url = surl.scheme + "://" + surl.host + "/" + application_id + "/"
        logger.debug(base_url)
        return base_url
    
    ###########################################################################
    # Pilot Data Service related methods
    
    @classmethod  
    def add_pds(cls, application_url, pds):
        pds_url_no_dbtype = cls.get_pds_url(application_url, pds.id)
        pds_url = cls.__get_url(pds_url_no_dbtype)
        
        return pds_url_no_dbtype
    
    
    @classmethod
    def delete_pds(cls, pds_url):
        pds_url = cls.__get_url(pds_url)
       
       
    ###########################################################################
    # Pilot Data related methods    
    @classmethod
    def add_pd(cls, pds_url, pd):
        pds_url = cls.__remove_dbtype(pds_url)        
        pd_url =pds_url+"/" + pd.id
        pd_description_url = cls.__get_url(pd_url + "/description")
        logger.debug("PDS URL: %s, PD Description URL: %s"%(pds_url, pd_description_url))
        # directory is recursively created
        #pd_desc_entry = saga.advert.entry(saga.url(pd_description_url),
        #                                   saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        #logger.debug("initialized advert entry for pds: " + pd_description_url)
        #pd_desc_entry.store_string(json.dumps(pd.data_unit_description))
        return pd_url
    
    @classmethod
    def update_pd(cls, pd):
        if len(pd.data_units) > 0:
            du_urls = [i.url for i in pd.data_units.values()]
            #cls.__store_entry(cls.__remove_dbtype(pd.url)+"/data-units", du_urls)
        #cls.__store_entry(cls.__remove_dbtype(pd.url)+"/pilot-data", pd.to_dict())
    
    
    @classmethod
    def get_pd(cls, pds_url):
        logger.debug("GET PD: " + pds_url)     
        pd_dict={}        
        #pd_dict["pilot_data" ]=  cls.__retrieve_entry(cls.__remove_dbtype(pds_url)+"/pilot-data")
        #pd_dict["pilot_data"] = cls.__retrieve_entry(cls.__remove_dbtype(pds_url)+"/pilot-data") 
        #return pd_dict
        
    
    @classmethod
    def list_pd(cls, pds_url):
        """ return a list of urls to pd managed by the PDS """
        pds_url = cls.__get_url(pds_url)
        logger.debug("List PD at %s"%pds_url)
        #pds_dir = saga.advert.directory(pds_url, saga.advert.Create | 
        #                               saga.advert.CreateParents | 
        #                               saga.advert.ReadWrite)
        
        #pd_list = pds_dir.list()
        #pd_full_urls = []
        #for i in pd_list:
        #    pd_full_urls.append(pds_url + "/" + i)   
        #return pd_full_urls
    
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
        pds_urls = [cls.__remove_dbtype(i.url) for i in cds.pilot_data_services]
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
        cds_url = cls.__get_url(cls.__remove_dbtype(cds_url))
       
        
    
    
    ###########################################################################
    # URL Tweaking
    
    @classmethod
    def get_pds_url(cls, application_url, pds_id):
        pds_url = application_url+RedisCoordinationAdaptor.PILOT_DATA_SERVICE_PATH+"/"+pds_id        
        logger.debug("PDS URL: %s"%(pds_url))
        return pds_url
    
    @classmethod
    def get_cds_url(cls, application_url, cds_id):
        cds_url = application_url+RedisCoordinationAdaptor.COMPUTE_DATA_SERVICE_PATH+"/"+cds_id        
        logger.debug("CDS URL: %s"%(cds_url))
        return cds_url
    
    ###########################################################################
    # internal methods
    
    @classmethod
    def __get_url(cls, url):
        return url
    
    @classmethod
    def __remove_dbtype(cls, url):
        surl = SAGAUrl(url)
        return str(surl)
        
    @classmethod
    def __store_entry(cls, entry_url, content):
        entry_url = cls.__get_url(entry_url)
        
        
    @classmethod
    def __retrieve_entry(cls, entry_url):
        entry_url = cls.__get_url(entry_url)
        