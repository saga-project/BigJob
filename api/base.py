""" BigJob API

This Module contains the API of the BigJob framework

All BigJob implementation must be derived from this API.

"""

class bigjob(object):
       
    def __init__(self, database_host):  
        pass
    
    def start_pilot_job(self, 
                 lrms_url, 
                 bigjob_agent_executable,
                 number_nodes,
                 queue,
                 project,
                 working_directory,
                 userproxy,
                 walltime,
                 processes_per_node=1):
        pass
         
    def get_state(self):        
        pass
    
    def get_state_detail(self): 
        pass
    
    def cancel(self):        
        pass
    
    def wait(self):        
        pass
                    
                    
class subjob(object):
    
    def __init__(self, database_host=None):
        pass
    
    def submit_job(self, pilot_url, jd):
        pass
    
    def get_state(self):        
        pass
    
    def cancel(self):
        pass
