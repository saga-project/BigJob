import logging
import saga
import json
import pdb

from pilot import *
from bigjob import logger

class AdvertCoordinationAdaptor:
    """
        BigData persists its data in a central data space, e.g. the Advert service
        to facilitate distributed coordination:
        
        advert://advert.cct.lsu.edu/pilot/3d0d5960-296d-11e1-8896-00264a13ca4c/data/ => namespace for pilot data
        
        advert://advert.cct.lsu.edu/pilot/3d0d5960-296d-11e1-8896-00264a13ca4c/data/pds => pilot data service
        advert://advert.cct.lsu.edu/pilot/3d0d5960-296d-11e1-8896-00264a13ca4c/data/pds/pilot-data-description  => pilot data description
        ...
        
        
        advert://advert.cct.lsu.edu/pilot/3d0d5960-296d-11e1-8896-00264a13ca4c/data/pds/ => pilot store service
        advert://advert.cct.lsu.edu/pilot/3d0d5960-296d-11e1-8896-00264a13ca4c/data/pds/pilot-data-description => pilot data description
    
        This class is stateless - the application's base_url needs to be passed into every method.    
    """
    BASE_URL="advert://localhost/"
    BASE_URL_QUERY_STRING="?dbtype=sqlite3"
    
    PILOT_PATH="pilot"
    PILOT_DATA_PATH=PILOT_PATH
    PILOT_DATA_SERVICE_PATH=PILOT_DATA_PATH+"/pds"
    DATA_UNIT_SERVICE_PATH=PILOT_DATA_PATH+"/dus"
    COMPUTE_DATA_SERVICE_PATH = PILOT_DATA_PATH + "/cds"

    
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
    def add_pds(cls, application_url, pds):
        pds_url_no_dbtype = cls.get_pds_url(application_url, pds.id)
        pds_url = cls.__get_url(pds_url_no_dbtype)
        logger.debug("Create PDS directory at %s"%pds_url)
        saga.advert.directory(pds_url, saga.advert.Create | 
                                       saga.advert.CreateParents | 
                                       saga.advert.ReadWrite)
        return pds_url_no_dbtype
    
    
    @classmethod
    def delete_pds(cls, pds_url):
        pds_url = cls.__get_url(pds_url)
        pds_dir = saga.advert.directory(saga.url(pds_url), 
                                        saga.advert.Create | 
                                        saga.advert.CreateParents | 
                                        saga.advert.ReadWrite)
        pds_dir.remove(pds_url, saga.name_space.Recursive)  
    
    ###########################################################################
    # Pilot Data related methods
    
    @classmethod
    def add_pd(cls, pds_url, pd):
        pds_url = cls.__remove_dbtype(pds_url)        
        pd_url =pds_url+"/" + pd.id
        pd_description_url = cls.__get_url(pd_url + "/description")
        logger.debug("PDS URL: %s, PD Description URL: %s"%(pds_url, pd_description_url))
        # directory is recursively created
        pd_desc_entry = saga.advert.entry(saga.url(pd_description_url),
                                           saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        logger.debug("initialized advert entry for pds: " + pd_description_url)
        pd_desc_entry.store_string(json.dumps(pd.data_unit_description))
        return pd_url
    
    @classmethod
    def update_pd(cls, pd):
        if len(pd.data_units) > 0:
            du_urls = [i.url for i in pd.data_units.values()]
            cls.__store_entry(cls.__remove_dbtype(pd.url)+"/data-units", du_urls)
        cls.__store_entry(cls.__remove_dbtype(pd.url)+"/pilot-data", pd.to_dict())
    
    
    @classmethod
    def get_pd(cls, pds_url):
        logger.debug("GET PD: " + pds_url)     
        pd_dict={}        
        #pd_dict["pilot_data" ]=  cls.__retrieve_entry(cls.__remove_dbtype(pds_url)+"/pilot-data")
        pd_dict["pilot_data"] = cls.__retrieve_entry(cls.__remove_dbtype(pds_url)+"/pilot-data") 
        return pd_dict
        
    
    @classmethod
    def list_pd(cls, pds_url):
        """ return a list of urls to pd managed by the PDS """
        pds_url = cls.__get_url(pds_url)
        logger.debug("List PD at %s"%pds_url)
        pds_dir = saga.advert.directory(pds_url, saga.advert.Create | 
                                       saga.advert.CreateParents | 
                                       saga.advert.ReadWrite)
        
        pd_list = pds_dir.list()
        pd_full_urls = []
        for i in pd_list:
            pd_full_urls.append(pds_url + "/" + i)   
        return pd_full_urls
    
    @classmethod
    def delete_pd(cls, pds_url):
        pds_url = cls.__get_url(pds_url)
        pd_dir = saga.advert.directory(saga.url(pds_url), 
                                        saga.advert.Create | 
                                        saga.advert.CreateParents | 
                                        saga.advert.ReadWrite)
        pd_dir.remove(pds_url, saga.name_space.Recursive)  
    
        
    ###########################################################################
    # Compute Data Service related methods
    @classmethod  
    def add_cds(cls, application_url, cds):
        cds_url_no_dbtype = cls.get_cds_url(application_url, cds.id)
        cds_url = cls.__get_url(cds_url_no_dbtype)
        logger.debug("Create CDS directory at %s"%cds_url)
        saga.advert.directory(cds_url, saga.advert.Create | 
                                       saga.advert.CreateParents | 
                                       saga.advert.ReadWrite)
        return cds_url_no_dbtype
    
    @classmethod  
    def update_cds(cls, cds_url, cds):
        
        # Storage and Compute Resources
        pds_urls = [cls.__remove_dbtype(i.url) for i in cds.pilot_data_services]
        cls.__store_entry(cls.__remove_dbtype(cds_url)+"/pds/", pds_urls)
        
        pjs_urls = [i.url for i in cds.pilot_job_services]
        cls.__store_entry(cls.__remove_dbtype(cds_url)+"/cds/", pjs_urls)
        
        # currently managed PDs and WUs
        pd_urls = [i.url for i in cds.data_units.values()]
        cls.__store_entry(cls.__remove_dbtype(cds_url)+"/du/", pd_urls)
        
        wu_urls = [i.url for i in cds.compute_units.values()]
        cls.__store_entry(cls.__remove_dbtype(cds_url)+"/cu/", wu_urls)
            
        
    @classmethod
    def delete_cds(cls, cds_url):
        cds_url = cls.__get_url(cls.__remove_dbtype(cds_url))
        cds_dir = saga.advert.directory(saga.url(cds_url), 
                                        saga.advert.Create | 
                                        saga.advert.CreateParents | 
                                        saga.advert.ReadWrite)
        # cds_dir.remove(cds_url, saga.name_space.Recursive)
    
    
    
        
    ###########################################################################
    #  Data Unit related methods
    @classmethod
    def add_du(cls, dus_url, du):
        du_url = cls.__remove_dbtype(dus_url)  +  "/" + du.id     
        du_url = cls.__get_url(du_url)
        # directory is recursively created
        #saga.advert.directory(saga.url(du_url),
        #                                   saga.advert.Create | saga.advert.CreateParents | saga.advert.ReadWrite)
        #logger.debug("initialized advert entry for dus: " + du_url)
        return du_url

    
    @classmethod
    def get_du(cls, du_url):
        logger.debug("**** GET PD: " + du_url)
        du_dict={}        
        du_dict["data_unit_description" ]=  cls.__retrieve_entry(cls.__remove_dbtype(du_url)+"/description")
        du_dict["state"] = cls.__retrieve_entry(cls.__remove_dbtype(du_url)+"/state") 
        du_dict["data_units"] = cls.__retrieve_entry(cls.__remove_dbtype(du_url)+"/data-units")
        du_dict["pilot_data"] =  cls.__retrieve_entry(cls.__remove_dbtype(du_url)+"/pilot-data")    
        logger.debug("Open pilot data at: " + du_url + " State: " + str(du_dict))     
        return du_dict
    
     
    @classmethod  
    def update_du(cls, du):
        logger.debug("**** Update pilot data at: " + du.url)
        cls.__store_entry(cls.__remove_dbtype(du.url)+"/description", du.data_unit_description)
        cls.__store_entry(cls.__remove_dbtype(du.url)+"/state", du.state)
                
        du_urls = [i.url for i in du.pilot_data]
        cls.__store_entry(cls.__remove_dbtype(du.url)+"/pilot-data", du_urls)
                
        du_dict_list = [i.to_dict() for i in du.data_unit_items]
        cls.__store_entry(cls.__remove_dbtype(du.url)+"/data-units", du_dict_list)
        
       
    @classmethod
    def list_du(cls, dus_url):
        """ return a list of urls to du managed by the PDS """
        dus_url = cls.__get_url(dus_url)
        logger.debug("List PDS at %s"%dus_url)
        dus_dir = saga.advert.directory(dus_url, saga.advert.Create | 
                                       saga.advert.CreateParents | 
                                       saga.advert.ReadWrite)
        
        du_list = dus_dir.list()
        du_full_urls = []
        for i in du_list:
            du_full_urls.append(dus_url + "/" + i)   
        return du_full_urls
    
    
    @classmethod
    def delete_du(cls, du_url):
        du_url = cls.__get_url(du_url)
        du_dir = saga.advert.directory(saga.url(du_url), 
                                        saga.advert.Create | 
                                        saga.advert.CreateParents | 
                                        saga.advert.ReadWrite)
        du_dir.remove(du_url, saga.name_space.Recursive)  
    
    
    
    ###########################################################################
    # URL Tweaking
    
    @classmethod
    def get_pds_url(cls, application_url, pds_id):
        pds_url = application_url+AdvertCoordinationAdaptor.PILOT_DATA_SERVICE_PATH+"/"+pds_id        
        logger.debug("PDS URL: %s"%(pds_url))
        return pds_url
    
    @classmethod
    def get_cds_url(cls, application_url, cds_id):
        cds_url = application_url+AdvertCoordinationAdaptor.COMPUTE_DATA_SERVICE_PATH+"/"+cds_id        
        logger.debug("CDS URL: %s"%(cds_url))
        return cds_url
    
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
        #logger.debug("Store Advert entry at: " + entry_url 
        #              + " Content: " + str(json.dumps(content)))
        
    @classmethod
    def __retrieve_entry(cls, entry_url):
        entry_url = cls.__get_url(entry_url)
        #logger.debug("Retrieve Advert entry at: " + entry_url)
        # directory is recursively created
        entry = saga.advert.entry(saga.url(entry_url),
                                           saga.advert.Create | 
                                           saga.advert.CreateParents | saga.advert.ReadWrite)
        content = json.loads(entry.retrieve_string())
        #logger.debug("Retrieve Advert entry at: " + entry_url 
        #              + " Content: " + str(json.dumps(content)))
        return content
