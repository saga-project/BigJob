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
    """ B{PilotComputeDescription (PCD).}
        
        A PilotComputeDescription is a based on the attributes defined on 
        the SAGA Job Description.

        The PilotComputeDescription is used by the application to specify 
        what kind of PilotJobs it requires.
        
        Example::
             pilot_compute_description = {
                             "service_url": 'fork://localhost',
                             "number_of_processes": 1,                             
                             "working_directory": "/tmp/pilot-compute/",
                             'affinity_datacenter_label': "eu-de-south",              
                             'affinity_machine_label': "mymachine-1" 
                            }
        
        B{Attention}: The PilotComputeDescription is mapped 1:1 to the underlying SAGA-Python (Bliss) 
        job description, which is used for launching the pilot. Depending on the resource, it is required
        to add additional attributes, e.g. some XSEDE/Torque resources require the specification
        of both number_of_process and processes_per_node:
        
        Example::
              pilot_compute_description = {
                             "service_url": 'pbs+ssh://india.futuregrid.org',
                             "number_of_processes": 8,
                             "processes_per_node":8,                     
                             "working_directory": "/N/u/luckow",
                             'affinity_datacenter_label': "us-east-indiana",              
                             'affinity_machine_label': "india" 
                            }
    
        
         
    """

    # Class members
    __slots__ = (
        # Pilot / Agent description
        'service_url',
        'project',
        'working_directory',
        # I/O
        'input',
        'error',
        'output',
        'file_transfer',
        
        # Parallelism
        'number_of_processes',      # Total number of processes to start
        'processes_per_host',       # Nr of processes per host
        
        # Requirements
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
    """ B{PilotCompute (PC)}. 
    
        This is the object that is returned by the PilotComputeService when a 
        new PilotCompute (aka Pilot-Job) is created based on a PilotComputeDescription.

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
    """  B{PilotComputeService (PCS).}
    
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


    def create_pilot(self, pilotcompute_description):
        """ Add a PilotCompute to the PilotComputeService

            Keyword arguments:
            pilotcompute_description -- PilotCompute Description

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
    """ B{ComputeUnitService (CUS).}
    
        Please use ComputeDataService!
        
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
    """ B{ComputeUnitDescription (CUD).}
    
        The ComputeUnitDescription is a task description based on 
        SAGA Job Description. 
        
        It offers the application to describe a L{ComputeUnit} in an abstract 
        way that is dealt with by the Pilot-Manager. It can contain
        references to depended L{DataUnit}s. ComputeUnitDescription are submitted
        to the L{ComputeDataService}.
        
        Format::
        
            compute_unit_description =            
                {
                    'executable': <path to executable>,  
                    'arguments': <arguments>,           # Arguments 
                    'environment': <environment>,          # environment variables 
                    
                    # Working directory
                    # Recommendation: Do not set working directory!
                    #     if None working directory is sandbox directory of this CU (automatically
                    #     created by BigJob)
                    'working_directory': <working directory>,  
                    
                    # I/O
                    'input': <stdin>,
                    'error': <sterr>,
                    'output': <stdout>,
                                
                    # Parallelism
                    
                    # Defines how many CPU cores are reserved for the application process.
                    'number_of_processes': <Total number of processes to start>,
                    
                    # Defines how the application process is launched:
                    #     "single": ./a.out
                    #     "mpi": mpirun -np <number_of_processes> ./a.out
                    # In the MPI case BJ generates an appropriate machinefile
                    'spmd_variation':      <Type and startup mechanism. Supported Values: [single, mpi]>,
                    
                    
                    # Data - input/output data flow for ComputeUnit
                    'input_data': [<data unit url>, ... ],      
                    'output_data': [<data unit url>, ... ]
                }
            
            Example::
                    compute_unit_description = {
                            "executable": "/bin/cat",
                            "arguments": ["test.txt"],
                            "number_of_processes": 1,
                            "output": "stdout.txt",
                            "error": "stderr.txt",   
                            "input_data" : [data_unit.get_url()], # this stages the content of the data unit to the working directory of the compute unit
                            "affinity_datacenter_label": "eu-de-south",              
                            "affinity_machine_label": "mymachine-1" 
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
    """ B{ComputeUnit (CU).}
    
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
