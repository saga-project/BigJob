""" 
This file contains the API for the PilotData Framework.
"""
    
class PilotDataDescription(dict):
    """ B{PilotDataDescription (PDD).}
    
        Dictionary based description containing reference to resource for Pilot Data.
        
        Example::
            
            {
            'service_url': "ssh://localhost/tmp/pilotstore/",
            'size':100,
                
            # Affinity
            'affinity_datacenter_label',    # pilot stores sharing the same label are located in the same data center          
            'affinity_machine_label',       # pilot stores sharing the same label are located on the same machine                           
            }
            
        Alternatively, a regular Python dictionary can be used.
    """
    
    def __init__(self):
        pass
    

    def __setattr__(self, attr, value):
        self[attr]=value
        
    
    def __getattr__(self, attr):
        return self[attr]



class PilotData(object):
    """ B{PilotData (PD)}.  """

    # Class members
    __slots__ = (
        'id',           # Reference to this PJ
        'description',  # Description of PilotData
        'context',      # SAGA context
        'resource_url', # Resource  URL       
        'state',        # State of the PilotData
        'state_detail', # Adaptor specific state of the PilotData       
    )
    
    def __init__(self):
        raise  NotImplementedError("Abstract super class, please use PilotData implementation class in pilot namespace")


    def cancel(self):        
        """ Cancel PilotData 

            Keyword arguments:
            None
        """
        pass
    
    
    def get_state(self):
        pass
    
       


class PilotDataService(object):
    """ B{PilotDataService (PDS).} """

    # Class members
    __slots__ = (
        'id',           # Reference to this PJS
        'state',       # Status of the PJS
        'pilot_data'    # List of PJs under this PJS
    )

    def __init__(self, pss_id=None):
        """ Create a PilotDataService

            Keyword arguments:
            pss_id -- restore from pss_id
        """
        raise  NotImplementedError("Abstract super class, please use PilotDataService implementation class in pilot namespace")


    def create_pilot(self, pilot_data_description):
        """ Create a PilotData 

            Keyword arguments:
            pilot_data_description -- PilotData Description            
            
            Return value:
            A PilotData handle
        """
        pass

    def list_pilots(self):
        """ List all PDs of PDS """
        pass

    def cancel(self):
        """ Cancel the PilotDataService.
            
            Keyword arguments:
            None

            Return value:
            Result of operation
        """
        pass


#
# PilotDataService
# 
class DataUnitService(object):
    """ B{DataUnitService (DUS).}
    
        Please use ComputeDataService
    """
   
    def __init__(self, pds_id=None):
        """ Create a DataUnitService.

            Keyword arguments:
            pds_id -- Reconnect to an existing DataUnitService 
        """
        raise  NotImplementedError("Abstract super class, please use DataUnitService implementation class in pilot namespace")


    def add_pilot_data_service(self, pss):
        """ Add a PilotDataService 

            Keyword arguments:
            pss -- The PilotDataService to add.

            Return:
            Result
        """
        pass

    def remove_pilot_data_service(self, pss):
        """ Remove a PilotDataService 

            
            Keyword arguments:
            pss -- The PilotDataService to remove 
            Return:
            Result
        """
        pass
    
    
    def list_pilot_data(self):
        """ List all PDs of PDS """
        pass
    

    def submit_pilot_data_set(self, data_unit_description):
        """ Create Pilot Data object and schedule it to a Pilot Data """
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
# DataUnitDescription
# 
class DataUnitDescription(dict):
    """ B{DataUnitDescription (DUD).} 
    
        Description object for the creation of L{DataUnit}s::
    
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
# DataUnit 
# 
class DataUnit(object):
    """ B{DataUnit (DU).} 
    
        Holds a set of files (that generally share the same locality and are used together).        
    """

    # Class members
    __slots__ = (
        'id',               # Reference 
        'description',      # Description
        'state',            # State
        'file_list'        # files managed by PilotData object
    )

    def cancel(self):
        """ Cancel the PD. """
        raise  NotImplementedError("Abstract super class, please use DataUnit implementation class in pilot namespace")
  
        
    def get_state(self):
        """
            get current state of Pilot Data:
                New => Initialized
                Pending => Files are synchronized with a pilot store
                Running => PD is in sync with all replicas
                Done => Terminated
        """
        pass
    
    
    def wait(self):
        """ Wait until in Running state 
            (or Failed state)
        """
        pass
    
    def export(self, target_directory):
        """ copies content of PD to a directory on the local machine"""
        pass    
        
