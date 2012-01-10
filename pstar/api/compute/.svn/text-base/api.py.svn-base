""" 
This file contains the API for the TROY PilotJob Framework.
It is not supposed to be used directly by applications. 
"""

class State(object):
    Unknown = "Unknown"
    New = "New"
    Running = "Running"
    Done = "Done"
    Canceled = "Canceled"
    Failed = "Failed"
    Pending = "Pending"
    


class PilotJobDescription(dict):
    """ TROY PilotJobDescription.
        
        A PilotJobDescription is a based on the SAGA Job Description.

        The PilotJobDescription is used by the application to specify 
        what kind of PilotJobs it requires.
    """

    # Class members
    __slots__ = (
        # Pilot / Agent description
        'executable',
        'arguments',
        'cleanup',
        'environment',
        'interactive',
        'contact',
        'project',
        'start_time',
        'working_directory',
        # I/O
        'input',
        'error',
        'output',
        'file_transfer',
        # Parallelism
        'number_of_processes',      # Total number of processes to start
        'processes_per_host',       # Nr of processes per host
        'threads_per_process',      # Nr of threads to start per process
        'total_core_count',         # total number of cores requested
        'spmd_variation',           # Type and startup mechanism
        
        # Requirements
        'candidate_hosts',
        'cpu_architecture',
        'total_physical_memory', 
        'operating_system_type',
        'total_cpu_time',
        'wall_time_limit',
        'queue',
        
        # Affinity
        'affinity_datacenter_label',    # pilot jobs sharing the same label are located in the same data center          
        'affinity_machine_label',       # pilot jobs sharing the same label are located on the same machine
    )

    def __init__(self):
        pass
    
    
    def __setattr__(self, attr, value):
        self[attr]=value
        
    
    def __getattr__(self, attr):
        return self[attr]
    
    

class PilotJob(object):
    """ TROY PilotJob. 
    
        This is the object that is returned by the PilotJobService when a 
        new PilotJob is created based on a PilotJobDescription.

        The PilotJob object can be used by the application to keep track 
        of PilotJobs that are active.
        
        A PilotJob has state, can be queried, can be cancelled and be 
        re-initialized.
    """

    # Class members
    __slots__ = (
        'id',           # Reference to this PJ
        'description',  # Description of PilotJob
        'context',      # SAGA context
        'rm',           # Resource Manager URL
        'pj_type',      # Type of TROY backend
        'state',        # State of the PilotJob
        'state_detail', # Adaptor specific state of the PilotJob
        'callback',     # Callback object
        'wall_time_left'      # Remaining wallclock time left
    )

    def cancel(self):        
        """ Remove the PilotJob from the PilotJob Service.

            Keyword arguments:
            None
        """
        pass

    def reinitialize(self, pilotjob_description):        
        """ Re-Initialize the PilotJob to the (new) PilotJobDescription.
        
            Keyword arguments:
            pilotjob_description -- A PilotJobDescription
        """
        pass

    def set_callback(self, member, cb):
        """ Set a callback function for a member.

            Keyword arguments:
            member -- The member to set the callback for (state / state_detail).
            cb -- The callback object to call.
        """
        pass

    def unset_callback(self, member):
        """ Unset a callback function from a member

            Keyword arguments:
            member -- The member to unset the callback from.
        """
        pass
    
    def get_state(self):
        pass
#
# TROY PilotJob Service
#
class PilotJobService(object):
    """ TROY PilotJobService.
    
        The PilotJobService is responsible for creating and managing 
        the PilotJobs.

        It is the application's interface to the Pilot-Manager in the 
        P* Model.
        
    """

    # Class members
    __slots__ = (
        'id',           # Reference to this PJS
        'state',       # Status of the PJS
        'pilot_jobs'    # List of PJs under this PJS
    )

    def __init__(self, pjs_id=None):
        """ Create a PilotJobService object.

            Keyword arguments:
            pjs_id -- Don't create a new, but connect to an existing (optional)
        """
        pass

    def create_pilotjob(self, rm, pilotjob_description, pj_type=None, context=None):
        """ Add a PilotJob to the PilotJobService

            Keyword arguments:
            rm -- Contact string for the resource manager
            pilotjob_desc -- PilotJob Description
            pj_type -- TROY backend type (optional)
            context -- Security context (optional)

            Return value:
            A PilotJob handle
        """
        pass

    def cancel(self):
        """ Cancel the PilotJobService.

            This also cancels all the PilotJobs that were under control of this PJS.

            Keyword arguments:
            None

            Return value:
            Result of operation
        """
        pass





#
# TROY WorkUnitService
# 
class WorkUnitService(object):
    """ TROY WorkUnitService.
    
        The WorkUnitService is the application's interface to submit 
        WorkUnits to the Pilot-Manager in the P* Model.

        It can provide the application with a list of WorkUnits that are 
        managed by the Pilot-Manager.

        The WorkUnitService is linked to a PilotJobService for the actual 
        execution of the WorkUnits.
    """

    def __init__(self, wus_id=None):
        """ Create a Work Service object.
    
            Keyword arguments:
            wus_id -- Reconnect to an existing WUS (optional).
        """
        pass


    def add_pilot_job_service(self, pjs):
        """ Add a PilotJobService to this WUS.

            Keyword arguments:
            pilotjob_services -- The PilotJob Service(s) to which this 
                                 Work Unit Service will connect.

            Return:
            Result
        """
        pass


    def remove_pilot_job_service(self, pjs):
        """ Remove a PilotJobService from this WUS.

            Note that it won't cancel the PilotJobService, it will just no
            longer be connected to this WUS.

            Keyword arguments:
            pilotjob_services -- The PilotJob Service(s) to remove from this
                                 Work Unit Service. 

            Return:
            Result
        """
        pass


    def submit_work_unit(self, wud):
        """ Submit a WU to this Work Unit Service.

            Keyword argument:
            wud -- The WorkUnitDescription from the application

            Return:
            WorkUnit object
        """
        pass


    def cancel(self):
        """ Cancel the WUS.
            
            Cancelling the WUS also cancels all the WUs submitted to it.
    
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
# TROY WorkUnitDescription
# 
class WorkUnitDescription(dict):
    """ TROY WorkUnitDescription.
    
        The WorkUnitDescription is a job/task/call description based on 
        SAGA Job Description. 
        
        It offers the application to describe a WorkUnit in an abstract 
        way that is dealt with by the Pilot-Manager.
    """

    # Class members
    __slots__ = (
        # Action description
        'executable',           # The "action" to execute
        'arguments',            # Arguments to the "action"
        'cleanup',
        'environment',          # "environment" settings for the "action"
        'interactive', 
        'contact',
        'project',
        'start_time',
        'working_directory',
        # I/O
        'input',
        'error',
        'output',
        'file_transfer',
        # Parallelism
        'number_of_processes',  # Total number of processes to start
        'processes_per_host',   # Nr of processes per host
        'threads_per_process',  # Nr of threads to start per process
        'total_core_count',     # Total number of cores requested
        'spmd_variation',       # Type and startup mechanism
        # Requirements
          'candidate_hosts',
        'cpu_architecture',
        'total_physical_memory',
        'operating_system_type',
        'total_cpu_time',
        'wall_time_limit',
        'queue'
    )

    def __init__(self):
        pass

    def __setattr__(self, attr, value):
        self[attr]=value
        
    
    def __getattr__(self, attr):
        return self[attr]


#
# TROY WorkUnit(WU)
# 
class WorkUnit(object):
    """ TROY WorkUnit.
    
        This is the object that is returned by the WorkUnitService when a 
        new WorkUnit is created based on a WorkUnitDescription.

        The WorkUnit object can be used by the application to keep track 
        of WorkUnits that are active.

        A WorkUnit has state, can be queried and can be cancelled.
    """

    # Class members
    __slots__ = (
        'id',               # Reference to this WU
        'description',      # Description of this WU
        'state_detail',     # Detailed (application specific) state of this WU
        'callback'          # Callback object
    )

    def cancel(self):
        """ Cancel the WU. """
        pass

    def set_callback(self, member, cb):
        """ Set a callback function for a member.

            Keyword arguments:
            member -- The member to set the callback for (state / state_detail).
            cb -- The callback object to call.
        """
        pass

    def unset_callback(self, member):
        """ Unset a callback function from a member

            Keyword arguments:
            member -- The member to unset the callback from.
        """
        pass

    def get_state(self):
        pass
    
#
# TROY Callback (Abstract) Class
#
class Callback(object):
    """ Callback class.

        Specifies the structure for callback classes.

        Callbacks can be set for WorkUnits on the state or state_detail members.
    """

    def cb(self, wu, member, value):
        """ This is the method that needs to be implemented by the application
        
            Keyword arguments:
            wu -- The WU that is calling back.
            member -- The member that triggered the callback.
            value -- The new (detailed) state.

            Return:
            Keep -- Keep or remove the callback
        """
        pass
