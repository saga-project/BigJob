import logging
import saga
import json
import pdb

from bigdata import *


class AdvertCoordinationAdaptor:
    """
        BigData persists its data in a central data space, e.g. the Advert service
        to facilitate distributed coordination:
        
        advert://advert.cct.lsu.edu/pilot/3d0d5960-296d-11e1-8896-00264a13ca4c/data/ => namespace for pilot data
        
        advert://advert.cct.lsu.edu/pilot/3d0d5960-296d-11e1-8896-00264a13ca4c/data/pds => pilot data service
        advert://advert.cct.lsu.edu/pilot/3d0d5960-296d-11e1-8896-00264a13ca4c/data/pds/pilot-data-description  => pilot data description
        ...
        
        
        advert://advert.cct.lsu.edu/pilot/3d0d5960-296d-11e1-8896-00264a13ca4c/data/pss/ => pilot store service
        advert://advert.cct.lsu.edu/pilot/3d0d5960-296d-11e1-8896-00264a13ca4c/data/pss/pilot-store-description => pilot store description
    
        This class is stateless - the application's base_url needs to be passed into every method.    
    """
    BASE_URL="advert://localhost/"
    BASE_URL_QUERY_STRING="?dbtype=sqlite3"
    
    PILOT_PATH="pilot"
    PILOT_DATA_PATH=PILOT_PATH
    PILOT_STORE_SERVICE_PATH=PILOT_DATA_PATH+"/pss"
    PILOT_DATA_SERVICE_PATH=PILOT_DATA_PATH+"/pds"
    WORK_DATA_SERVICE_PATH = PILOT_DATA_PATH + "/wds"

    
    ###########################################################################
    # Construct a base url for an application
    
    @classmethod
    def get_base_url(cls, application_id):
        surl = saga.url(cls.BASE_URL)
        base_url = surl.scheme + "://" + surl.host + "/" + application_id + "/"
        logging.debug(base_url)
        return base_url
    
    ###########################################################################
    # Pilot Store Service related methods
    
    @classmethod  
    def add_pss(cls, application_url, pss):
        pss_url_no_dbtype = cls.get_pss_url(application_url, pss.id)
        pss_url = cls.__get_url(pss_url_no_dbtype)
        logger.debug("Create PSS directory at %s"%pss_url)
        saga.advert.directory(pss_url, saga.advert.Create | 
                                       saga.advert.CreateParents | 
                                       saga.advert.ReadWrite)
        return pss_url_no_dbtype
    
    
    @classmethod
    def delete_pss(cls, pss_url):
        pss_url = cls.__get_url(pss_url)
        pss_dir = saga.advert.directory(saga.url(pss_url), 
                                        saga.advert.Create | 
                                        saga.advert.CreateParents | 
                                        saga.advert.ReadWrite)
        pss_dir.remove(pss_url, saga.name_space.Recursive)  
    
    ###########################################################################
    # Pilot Store related methods
    
    @classmethod
    def add_ps(cls, pss_url, ps):
        pss_url = cls.__remove_dbtype(pss_url)        
        ps_url =pss_url+"/" + ps.id
        ps_description_url = cls.__get_url(ps_url + "/description")
        logger.debug("PSS URL: %s, PS Description URL: %s"%(pss_url, ps_description_url))
        # directory is recursively created
        ps_desc_entry = saga.advert.entry(saga.url(ps_description_url),
                                           saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        logger.debug("initialized advert entry for pss: " + ps_description_url)
        ps_desc_entry.store_string(json.dumps(ps.pilot_store_description))
        return ps_url
    
    @classmethod
    def update_ps(cls, ps):
        if len(ps.pilot_data) > 0:
            pd_urls = [i.url for i in ps.pilot_data.values()]
            cls.__store_entry(cls.__remove_dbtype(ps.url)+"/pilot-data", pd_urls)
        cls.__store_entry(cls.__remove_dbtype(ps.url)+"/pilot-store", ps.to_dict())
    
    
    @classmethod
    def get_ps(cls, ps_url):
        logger.debug("GET PS: " + ps_url)     
        ps_dict={}        
        #ps_dict["pilot_data" ]=  cls.__retrieve_entry(cls.__remove_dbtype(ps_url)+"/pilot-data")
        ps_dict["pilot_store"] = cls.__retrieve_entry(cls.__remove_dbtype(ps_url)+"/pilot-store") 
        return ps_dict
        
    
    @classmethod
    def list_ps(cls, pss_url):
        """ return a list of urls to ps managed by the PSS """
        pss_url = cls.__get_url(pss_url)
        logger.debug("List PS at %s"%pss_url)
        pss_dir = saga.advert.directory(pss_url, saga.advert.Create | 
                                       saga.advert.CreateParents | 
                                       saga.advert.ReadWrite)
        
        ps_list = pss_dir.list()
        ps_full_urls = []
        for i in ps_list:
            ps_full_urls.append(pss_url + "/" + i)   
        return ps_full_urls
    
    @classmethod
    def delete_ps(cls, ps_url):
        ps_url = cls.__get_url(ps_url)
        ps_dir = saga.advert.directory(saga.url(ps_url), 
                                        saga.advert.Create | 
                                        saga.advert.CreateParents | 
                                        saga.advert.ReadWrite)
        ps_dir.remove(ps_url, saga.name_space.Recursive)  
    
        
    ###########################################################################
    # Work Data Service related methods
    @classmethod  
    def add_wds(cls, application_url, wds):
        wds_url_no_dbtype = cls.get_wds_url(application_url, wds.id)
        wds_url = cls.__get_url(wds_url_no_dbtype)
        logger.debug("Create WDS directory at %s"%wds_url)
        saga.advert.directory(wds_url, saga.advert.Create | 
                                       saga.advert.CreateParents | 
                                       saga.advert.ReadWrite)
        return wds_url_no_dbtype
    
    @classmethod  
    def update_wds(cls, wds_url, wds):
        
        # Storage and Compute Resources
        pss_urls = [cls.__remove_dbtype(i.url) for i in wds.pilot_store_services]
        cls.__store_entry(cls.__remove_dbtype(wds_url)+"/pss/", pss_urls)
        
        pjs_urls = [i.url for i in wds.pilot_job_services]
        cls.__store_entry(cls.__remove_dbtype(wds_url)+"/pjs/", pjs_urls)
        
        # currently managed PDs and WUs
        pd_urls = [i.url for i in wds.pilot_data.values()]
        cls.__store_entry(cls.__remove_dbtype(wds_url)+"/pd/", pd_urls)
        
        wu_urls = [i.url for i in wds.work_units.values()]
        cls.__store_entry(cls.__remove_dbtype(wds_url)+"/wu/", wu_urls)
            
        
    @classmethod
    def delete_wds(cls, wds_url):
        wds_url = cls.__get_url(cls.__remove_dbtype(wds_url))
        wds_dir = saga.advert.directory(saga.url(wds_url), 
                                        saga.advert.Create | 
                                        saga.advert.CreateParents | 
                                        saga.advert.ReadWrite)
        # wds_dir.remove(wds_url, saga.name_space.Recursive)
    
    
    
        
    ###########################################################################
    # Pilot Data related methods
    @classmethod
    def add_pd(cls, pds_url, pd):
        pd_url = cls.__remove_dbtype(pds_url)  +  "/" + pd.id     
        pd_url = cls.__get_url(pd_url)
        # directory is recursively created
        #saga.advert.directory(saga.url(pd_url),
        #                                   saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        #logger.debug("initialized advert entry for pds: " + pd_url)
        return pd_url

    
    @classmethod
    def get_pd(cls, pd_url):
        logger.debug("**** GET PD: " + pd_url)
        pd_dict={}        
        pd_dict["pilot_data_description" ]=  cls.__retrieve_entry(cls.__remove_dbtype(pd_url)+"/description")
        pd_dict["state"] = cls.__retrieve_entry(cls.__remove_dbtype(pd_url)+"/state") 
        pd_dict["data_units"] = cls.__retrieve_entry(cls.__remove_dbtype(pd_url)+"/data-units")
        pd_dict["pilot_stores"] =  cls.__retrieve_entry(cls.__remove_dbtype(pd_url)+"/pilot-stores")    
        logger.debug("Open pilot data at: " + pd_url + " State: " + str(pd_dict))     
        return pd_dict
    
     
    @classmethod  
    def update_pd(cls, pd):
        logger.debug("**** Update pilot data at: " + pd.url)
        cls.__store_entry(cls.__remove_dbtype(pd.url)+"/description", pd.pilot_data_description)
        cls.__store_entry(cls.__remove_dbtype(pd.url)+"/state", pd.state)
                
        ps_urls = [i.url for i in pd.pilot_stores]
        cls.__store_entry(cls.__remove_dbtype(pd.url)+"/pilot-stores", ps_urls)
                
        du_dict_list = [i.to_dict() for i in pd.data_units]
        cls.__store_entry(cls.__remove_dbtype(pd.url)+"/data-units", du_dict_list)
        
       
    @classmethod
    def list_pd(cls, pss_url):
        """ return a list of urls to ps managed by the PSS """
        pds_url = cls.__get_url(pds_url)
        logger.debug("List PDS at %s"%pds_url)
        pds_dir = saga.advert.directory(pds_url, saga.advert.Create | 
                                       saga.advert.CreateParents | 
                                       saga.advert.ReadWrite)
        
        pd_list = pds_dir.list()
        pd_full_urls = []
        for i in pd_list:
            pd_full_urls.append(pss_url + "/" + i)   
        return pd_full_urls
    
    
    @classmethod
    def delete_pd(cls, pd_url):
        pd_url = cls.__get_url(pd_url)
        pd_dir = saga.advert.directory(saga.url(pd_url), 
                                        saga.advert.Create | 
                                        saga.advert.CreateParents | 
                                        saga.advert.ReadWrite)
        pd_dir.remove(pd_url, saga.name_space.Recursive)  
    
    
    
    ###########################################################################
    # URL Tweaking
    
    @classmethod
    def get_pss_url(cls, application_url, pss_id):
        pss_url = application_url+AdvertCoordinationAdaptor.PILOT_STORE_SERVICE_PATH+"/"+pss_id        
        logger.debug("PSS URL: %s"%(pss_url))
        return pss_url
    
    @classmethod
    def get_wds_url(cls, application_url, wds_id):
        wds_url = application_url+AdvertCoordinationAdaptor.WORK_DATA_SERVICE_PATH+"/"+wds_id        
        logger.debug("PDS URL: %s"%(wds_url))
        return wds_url
    
    ###########################################################################
    # internal methods
    
    @classmethod
    def __get_url(cls, url):
        """ appends advert querystring for dbtype to url """
        url = url + AdvertCoordinationAdaptor.BASE_URL_QUERY_STRING
        return url
    
    @classmethod
    def __remove_dbtype(cls, url):
        surl = saga.url(url)
        surl.query = "" 
        return surl.get_string()
        
    @classmethod
    def __store_entry(cls, entry_url, content):
        entry_url = cls.__get_url(entry_url)
        
        # directory is recursively created
        entry = saga.advert.entry(saga.url(entry_url),
                                           saga.advert.Create | 
                                           saga.advert.CreateParents | saga.advert.ReadWrite)
        entry.store_string(json.dumps(content))
        logger.debug("Store Advert entry at: " + entry_url 
                      + " Content: " + str(json.dumps(content)))
        
    @classmethod
    def __retrieve_entry(cls, entry_url):
        entry_url = cls.__get_url(entry_url)
        #logger.debug("Retrieve Advert entry at: " + entry_url)
        # directory is recursively created
        entry = saga.advert.entry(saga.url(entry_url),
                                           saga.advert.Create | 
                                           saga.advert.CreateParents | saga.advert.ReadWrite)
        content = json.loads(entry.retrieve_string())
        logger.debug("Retrieve Advert entry at: " + entry_url 
                      + " Content: " + str(json.dumps(content)))
        return content