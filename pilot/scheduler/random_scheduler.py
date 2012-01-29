""" Random Scheduler - selects random pilot store """
import random
import pdb
import logging

class Scheduler:
    
    def __init__(self):
        self.pilot_stores=[]
    
    def set_pilot_stores(self, pilot_stores):
        """ set resources which are used for scheduling """
        self.pilot_stores=pilot_stores
        
    def schedule(self):
        logging.debug("Schedule to PS - # Avail stores: %d"%len(self.pilot_stores))        
        if len(self.pilot_stores)!=0:
            return random.choice(self.pilot_stores)
        return None