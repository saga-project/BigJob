""" Affinity-aware scheduler that evaluates affinity labels and input/output data flow
    

"""
import random
import logging
from bigjob import logger

class Scheduler:
    
    def __init__(self):
        self.pilot_data=[]
        self.pilot_jobs=[]
    
    def set_pilot_data(self, pilot_data):
        """ set resources which are used for scheduling """
        self.pilot_data=pilot_data
        
    
    def set_pilot_jobs(self, pilot_jobs):
        """ set resources which are used for scheduling """
        self.pilot_jobs=pilot_jobs
    
        
    def schedule_pilot_data(self, data_unit_description=None):
        logger.debug("Schedule to PD - # Avail pilots: %d"%len(self.pilot_data))     
        candidate_pilot_data = []  
        if data_unit_description.has_key("affinity_datacenter_label") and data_unit_description.has_key("affinity_machine_label"):
            for i in self.pilot_data: 
                pilot_data_description = i.pilot_data_description
                if pilot_data_description.has_key("affinity_machine_label") and pilot_data_description.has_key("affinity_datacenter_label"):
                    if data_unit_description["affinity_datacenter_label"] == pilot_data_description["affinity_datacenter_label"]\
                    and data_unit_description["affinity_machine_label"] == pilot_data_description["affinity_machine_label"]:
                        candidate_pilot_data.append(i)
        
        if len(candidate_pilot_data) == 0:
            # No PD with requested affinity found
            # move data unit into a "possibly" remote pilot data
            logger.debug("A No pilot data w/ affinity found... Looking for alternative pilot.")
            candidate_pilot_data = self.pilot_data
            
        if len(candidate_pilot_data)>0:
            return random.choice(candidate_pilot_data)
        
        return None
    
    
    def schedule_pilot_job(self, compute_unit_description=None):
        """ Enforces affinity description: if no PJ is available with the right
            affinity, CU can't be scheduled.
        
        """    
        logger.debug("Schedule to PJ - # Avail PJs: %d"%len(self.pilot_jobs))
        candidate_pilot_jobs = []
        required_number_of_processes=1 
        if compute_unit_description.has_key("number_of_processes"):
            required_number_of_processes = int(compute_unit_description["number_of_processes"])
        
        if compute_unit_description.has_key("affinity_datacenter_label") and compute_unit_description.has_key("affinity_machine_label"):
            for i in self.pilot_jobs:
                free_nodes = i.get_free_nodes()
                logger.debug("BJ: %r State: %s Free nodes: %d"%(i, i.get_state(), free_nodes))
                if i.get_state()=="Running" and free_nodes >= required_number_of_processes: # check whether pilot is active
                    pilot_job_description = i.pilot_compute_description
                    if pilot_job_description.has_key("affinity_datacenter_label") and pilot_job_description.has_key("affinity_machine_label"):
                        if pilot_job_description["affinity_datacenter_label"] == compute_unit_description["affinity_datacenter_label"] and pilot_job_description["affinity_machine_label"] == compute_unit_description["affinity_machine_label"]:
                            candidate_pilot_jobs.append(i)
                        
                        
        if len(candidate_pilot_jobs) == 0:
            # No PJ with requested affinity found
            # move compute unit into a "possibly" remote pilot job
            logger.debug("B No pilot compute w/ affinity found... Looking for alternative pilot.")
            for i in self.pilot_jobs:                
                logger.debug("BJ: %r State: %s"%(i, i.get_state()))
                free_nodes = i.get_free_nodes()
                if i.get_state()=="Running" and free_nodes >= required_number_of_processes:
                    candidate_pilot_jobs.append(i)
                    #candidate_pilot_jobs=self.pilot_jobs

                
        logger.debug("Candidate PJs: %r"%(candidate_pilot_jobs))   
        if len(candidate_pilot_jobs)>0:
            return random.choice(candidate_pilot_jobs)
        
        return None
    
    def __check_pilot_data_dependency(self, work_unit_description):
        pilot_data_dependencies = work_unit_description["input_pilot_data"]
        for i in pilot_data_dependencies:
            pd = PilotData.pilot
            ps = i.get_pilot_data()
