import compute.api
import data.api


class WorkDataDescription(compute.api.WorkUnitDescription):
    """ TROY WorkUnitDescription.
        {
            'executable': <path to executable>,  
            'arguments': <arguments>,           # Arguments 
            'environment': <environment>,          # environment variables 
            
            # Working directory
            # A pilot data url will be translated to the local pilot store url, e.g.
            #     pilotdata://localhost/c2fafeae-03a9-11e1-9934-109addae22a3' 
            #     =>           
            #     ssh://localhost/tmp/pilotstore//c2fafeae-03a9-11e1-9934-109addae22a3
            #
            #     i.e. job is executed in /tmp/pilotstore/c2fafeae-03a9-11e1-9934-109addae22a3
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
            
            # Data - input/output data flow for WorkUnit
            'input_data': [<pilot data url>, ... ],      
            'output_data': [<pilot data url>, ... ]
        }
    """
    
    def __setattr__(self, attr, value):
        self[attr]=value
    
    def __getattr__(self, attr):
        return self[attr]

    


class WorkDataService(compute.api.WorkUnitService, data.api.PilotDataService):
    """ TROY WorkDataService.
    
        The WorkDataService is the application's interface to submit 
        WorkUnits and PilotData/DataUnit to the Pilot-Manager 
        in the P* Model.
        
    """
   

    def __init__(self, wds_id=None):
        """ Create a Work Data Service object.

            Keyword arguments:
            wds_id -- Reconnect to an existing WDS (optional).
        """
        pass   
  