""" Pilot Compute related entities."""

class State(object):
    """ B{State}.
    
        States used for L{PilotCompute}, L{PilotData}, L{ComputeUnit}, L{DataUnit} and L{ComputeDataService}
    """
    Unknown = "Unknown"
    New = "New"
    Running = "Running"
    Done = "Done"
    Canceled = "Canceled"
    Failed = "Failed"
    Pending = "Pending"
    


class PilotComputeDescription(dict):
    """ B{PilotComputeDescription.}
        
        A PilotComputeDescription is a based on the SAGA Job Description.

        The PilotComputeDescription is used by the application to specify 
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
    
    

class PilotCompute(object):
    """ PilotCompute (PilotJob). 
    
        This is the object that is returned by the PilotComputeService when a 
        new PilotCompute is created based on a PilotComputeDescription.

        The PilotCompute object can be used by the application to keep track 
        of PilotComputes that are active.
        
        A PilotCompute has state, can be queried, can be cancelled and be 
        re-initialized.
    """

    # Class members
    __slots__ = (
        'id',           # Reference to this PJ
        'description',  # Description of PilotCompute
        'context',      # SAGA context
        'rm',           # Resource Manager URL
        'pj_type',      # Type of TROY backend
        'state',        # State of the PilotCompute
        'state_detail', # Adaptor specific state of the PilotCompute
        'callback',     # Callback object
        'wall_time_left'      # Remaining wallclock time left
    )
    
    def __init__(self):
        raise  NotImplementedError("Abstract super class, please use PilotCompute implementation class in pilot namespace")

    def cancel(self):        
        """ Remove the PilotCompute from the PilotCompute Service.

            Keyword arguments:
            None
        """
        pass

    def reinitialize(self, pilotjob_description):        
        """ Re-Initialize the PilotCompute to the (new) PilotComputeDescription.
        
            Keyword arguments:
            pilotjob_description -- A PilotComputeDescription
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
#  Pilot ComputeService
#
class PilotComputeService(object):
    """  PilotComputeService.
    
        The PilotComputeService is responsible for creating and managing 
        the PilotComputes.

        It is the application's interface to the Pilot-Manager in the 
        P* Model.
        
    """

    #Class members
    __slots__ = (
        'id',           # Reference to this PJS
        'state',       # Status of the PJS
        'pilot_jobs'    # List of PJs under this PJS
    )
    
    

    def __init__(self, pjs_id=None):
        """ Create a PilotComputeService object.

            Keyword arguments:
            pjs_id -- Don't create a new, but connect to an existing (optional)
        """
        raise  NotImplementedError("Abstract super class, please use PilotComputeService implementation class in pilot namespace")


    def create_pilot(self, rm, pilotcompute_description, pj_type=None, context=None):
        """ Add a PilotCompute to the PilotComputeService

            Keyword arguments:
            rm -- Contact string for the resource manager
            pilotcompute_description -- PilotCompute Description
            pj_type --  backend type (optional)
            context -- Security context (optional)

            Return value:
            A PilotCompute handle
        """
        pass

    def cancel(self):
        """ Cancel the PilotComputeService.

            This also cancels all the PilotJobs that were under control of this PJS.

            Keyword arguments:
            None

            Return value:
            Result of operation
        """
        pass


#
#  ComputeUnitService
# 
class ComputeUnitService(object):
    """  ComputeUnitService.
    
        The ComputeUnitService is the application's interface to submit 
        ComputeUnits to the Pilot-Manager in the P* Model.

        It can provide the application with a list of ComputeUnits that are 
        managed by the Pilot-Manager.

        The ComputeUnitService is linked to a PilotComputeService for the actual 
        execution of the ComputeUnits.
    """

    def __init__(self, wus_id=None):
        """ Create a Work Service object.
    
            Keyword arguments:
            wus_id -- Reconnect to an existing WUS (optional).
        """
        raise  NotImplementedError("Abstract super class, please use ComputeUnitService implementation class in pilot namespace")



    def add_pilot_job_service(self, pjs):
        """ Add a PilotComputeService to this WUS.

            Keyword arguments:
            pilotjob_services -- The PilotCompute Service(s) to which this 
                                 Work Unit Service will connect.

            Return:
            Result
        """
        pass


    def remove_pilot_job_service(self, pjs):
        """ Remove a PilotComputeService from this WUS.

            Note that it won't cancel the PilotComputeService, it will just no
            longer be connected to this WUS.

            Keyword arguments:
            pilotjob_services -- The PilotCompute Service(s) to remove from this
                                 Work Unit Service. 

            Return:
            Result
        """
        pass


    def submit_compute_unit(self, wud):
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
#  ComuteUnitDescription
# 
class ComputeUnitDescription(dict):
    """ B{ComputeUnitDescription.}
    
        The ComputeUnitDescription is a task description based on 
        SAGA Job Description. 
        
        It offers the application to describe a L{ComputeUnit} in an abstract 
        way that is dealt with by the Pilot-Manager. It can contain
        references to depended L{DataUnit}s. ComputeUnitDescription are submitted
        to the L{ComputeDataService}.
        
        Example::
        
            compute_unit_description =            
                {
                    'executable': <path to executable>,  
                    'arguments': <arguments>,           # Arguments 
                    'environment': <environment>,          # environment variables 
                    
                    # Working directory
                    # A pilot data url will be translated to the local pilot store url, e.g.
                    #     pilotdata://localhost/c2fafeae-03a9-11e1-9934-109addae22a3' 
                    #     =>           
                    #     ssh://localhost/tmp/pilotdata/c2fafeae-03a9-11e1-9934-109addae22a3
                    #
                    #     i.e. job is executed in /tmp/pilotdata/c2fafeae-03a9-11e1-9934-109addae22a3
                    #     where file can be accessed using relative paths
                    'working_directory': <working directory>,  
                    
                    # I/O
                    'input': <stdin>,
                    'error': <sterr>,
                    'output': <stdout>,
                                
                    # Parallelism
                    'number_of_processes': <Total number of processes to start>,
                    'processes_per_host':  <Nr of processes per host>,
                    'threads_per_process': <Nr of threads to start per process>,
                    'total_core_count':    <Total number of cores requested>,
                    'spmd_variation':      <Type and startup mechanism>,
                    
                    # Requirements
                    'candidate_hosts': [<url>, ...],
                    'cpu_architecture': <architecture, e.g. x86_64>,
                    'total_physical_memory': <memory, e.g. 2000>,
                    'operating_system_type': <os, e.g. Linux>,
                    'total_cpu_time': <cpu time>,
                    'wall_time_limit': <walltime in sec, e.g. 600 (sec)>,            
                    
                    # Data - input/output data flow for ComputeUnit
                    'input_data': [<data unit url>, ... ],      
                    'output_data': [<data unit url>, ... ]
                }
            
        ComputeUnitDescription objects are loosely typed. A dictionary containing the respective keys
        can be passed instead to the L{ComputeDataService}.
    """
    def __init__(self):
        pass


    def __setattr__(self, attr, value):
        self[attr]=value
        
    
    def __getattr__(self, attr):
        return self[attr]


#
#  ComputeUnit(WU)
# 
class ComputeUnit(object):
    """  ComputeUnit.
    
        This is the object that is returned by the ComputeUnitService when a 
        new ComputeUnit is created based on a ComputeUnitDescription.

        The ComputeUnit object can be used by the application to keep track 
        of ComputeUnits that are active.

        A ComputeUnit has state, can be queried and can be cancelled.
    """

    # Class members
    __slots__ = (
        'id',               # Reference to this WU
        'description',      # Description of this WU
        'state_detail',     # Detailed (application specific) state of this WU
        'callback'          # Callback object
    )

    
    def cancel(self):
        """ Cancel the CU. """
        raise  NotImplementedError("Abstract super class, please use ComputeUnit implementation class in pilot namespace")


    
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
    
    
    def wait(self):
        """ Wait until in Done state 
            (or Failed state)
        """
#
#  Callback (Abstract) Class
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
