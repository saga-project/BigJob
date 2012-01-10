""" 
This file contains the API for the (proposed) TROY PilotData Framework.
"""
    
class PilotStoreDescription(dict):
    """ TROY PilotStoreDescription.
        {
            'service_url': "ssh://localhost/tmp/pilotstore/",
            'size':100,
            
            # Affinity
            'affinity_datacenter_label',    # pilot stores sharing the same label are located in the same data center          
            'affinity_machine_label',       # pilot stores sharing the same label are located on the same machine                           
        }    
    """
    
    def __init__(self):
        pass
    

    def __setattr__(self, attr, value):
        self[attr]=value
        
    
    def __getattr__(self, attr):
        return self[attr]



class PilotStore(object):
    """ TROY PilotStore.  """

    # Class members
    __slots__ = (
        'id',           # Reference to this PJ
        'description',  # Description of PilotStore
        'context',      # SAGA context
        'resource_url', # Resource  URL       
        'state',        # State of the PilotStore
        'state_detail', # Adaptor specific state of the PilotStore       
    )

    def cancel(self):        
        """ Cancel PilotStore 

            Keyword arguments:
            None
        """
        pass
    
    
    def get_state(self):
        pass
    
       


class PilotStoreService(object):
    """ TROY PilotStoreService (PSS).   """

    # Class members
    __slots__ = (
        'id',           # Reference to this PJS
        'state',       # Status of the PJS
        'pilot_stores'    # List of PJs under this PJS
    )

    def __init__(self, pss_id=None):
        """ Create a PilotStoreService

            Keyword arguments:
            pss_id -- restore from pss_id
        """
        pass

    def create_pilotstore(self, pilot_store_description):
        """ Create a PilotStore 

            Keyword arguments:
            pilot_store_description -- PilotStore Description            
            
            Return value:
            A PilotStore handle
        """
        pass

    def list_pilotstores(self):
        """ List all PSs of PSS """
        pass

    def cancel(self):
        """ Cancel the PilotStoreService.
            
            Keyword arguments:
            None

            Return value:
            Result of operation
        """
        pass


#
# TROY PilotDataService
# 
class PilotDataService(object):
    """ TROY PilotDataService (PDS).   
    """
   
    def __init__(self, pds_id=None):
        """ Create a PilotDataService.

            Keyword arguments:
            pds_id -- Reconnect to an existing PilotDataService 
        """
        pass

    def add_pilot_store_service(self, pss):
        """ Add a PilotStoreService 

            Keyword arguments:
            pss -- The PilotStoreService to add.

            Return:
            Result
        """
        pass

    def remove_pilot_store_service(self, pss):
        """ Remove a PilotStoreService 

            
            Keyword arguments:
            pss -- The PilotStoreService to remove 
            Return:
            Result
        """
        pass
    
    
    def list_pilot_data(self):
        """ List all PDs of PDS """
        pass
    

    def submit_pilot_data(self, pilot_data_description):
        """ Create Pilot Data object and schedule it to a Pilot Store """
        pass
    
    
    def cancel(self):
        """ Cancel the PDS.            
            
            Keyword arguments:
            None

            Return:
            Result
        """
        pass

    
    def get_state(self):
        pass
    
    
    def get_id(self):
        pass

#
# TROY PilotDataDescription
# 
class PilotDataDescription(dict):
    """ TROY PilotDataDescription.
        {
            'file_urls': [file1, file2, file3]        
        } 
        
        Currently, no directories supported
    """

    def __init__(self):
        pass

    def __setattr__(self, attr, value):
        self[attr]=value
    
    def __getattr__(self, attr):
        return self[attr]
    
    
#
# TROY PilotData 
# 
class PilotData(object):
    """ TROY PilotData. Holds a set of data units.        
    """

    # Class members
    __slots__ = (
        'id',               # Reference 
        'description',      # Description
        'state',            # State
        'data_units'        # DU managed by PilotData object
    )

    def cancel(self):
        """ Cancel the PD. """
        pass
    
    def add_data_unit(self, data_unit):
        pass
    
    def remove_data_unit(self, data_unit):
        pass
    
    def list_data_units(self):
        pass
        
    def get_state(self):
        """
            get current state of Pilot Data:
                New => Initialized
                Pending => Files are synchronized with a pilot store
                Running => PD is in sync with all replicas
                Done => Terminated
        """
        pass
    
    def export(self, target_directory):
        """ copies content of PD to a directory on the local machine"""
        pass    
        
    

class DataUnit(object):
    """ TROY DataUnit """

    __slots__ = (
        'id',        
        'url'
    )
    
    def __init__(self):
        pass
    
    
    
    