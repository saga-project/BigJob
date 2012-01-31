""" Random Scheduler - selects random pilot store """
import random
import pdb
import logging

class Scheduler:
    
    def __init__(self):
        self.pilot_data=[]
    
    def set_pilot_data(self, pilot_data):
        """ set resources which are used for scheduling """
        self.pilot_data=pilot_data
        
    def schedule(self):
        logging.debug("Schedule to PS - # Avail stores: %d"%len(self.pilot_data))        
        if len(self.pilot_data)!=0:
            return random.choice(self.pilot_data)
        return None