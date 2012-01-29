""" Random Scheduler - selects random pilot store """
import random
import pdb
import logging

class Scheduler:
    
    def __init__(self):
        self.pilot_stores=[]
        self.pilot_jobs=[]
    
    def set_pilot_stores(self, pilot_stores):
        """ set resources which are used for scheduling """
        self.pilot_stores=pilot_stores
        
    
    def set_pilot_jobs(self, pilot_jobs):
        """ set resources which are used for scheduling """
        self.pilot_jobs=pilot_jobs
    
        
    def schedule_pilot_data(self):
        logging.debug("Schedule to PS - # Avail stores: %d"%len(self.pilot_stores))        
        if len(self.pilot_stores)!=0:
            return random.choice(self.pilot_stores)
        return None
    
    
    def schedule_pilot_job(self, work_unit_description=None):
        logging.debug("Schedule to PJ - # Avail PJs: %d"%len(self.pilot_jobs))        
        if len(self.pilot_jobs)!=0:
            return random.choice(self.pilot_jobs)
        return None