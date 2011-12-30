import os
import logging
logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='bigjob')



version = "latest"

try:
    fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", 'VERSION')
    version = open(fn).read().strip()
    logger.debug("Loading BigJob version: " + version)
except IOError:
    pass

#READ config
import ConfigParser
CONFIG_FILE="bigjob.conf"
conf_file = os.path.dirname(os.path.abspath( __file__ )) + "/../" + CONFIG_FILE
config = ConfigParser.ConfigParser()
logger.debug ("read configfile: " + conf_file)
config.read(conf_file)
default_dict = config.defaults()        
saga = default_dict["saga"]

if saga.lower() == "bliss":
    SAGA_BLISS=True
else:
    SAGA_BLISS=False 


# define external-facing API
from bigjob.bigjob_manager import bigjob as myBigjob
from bigjob.bigjob_manager import subjob as mySubjob
from bigjob.bigjob_manager import description as myDescription

class subjob(mySubjob):
    pass


class bigjob(myBigjob):
    pass


class description(myDescription):
    pass

#    def __init__(self):
#        self.executable = ""
#        self.number_of_processes = ""
#        self.spmd_variation = ""
#        self.arguments = []
#        self.working_directory = ""
#        self.output = ""
#        self.error = ""
#        self.filetransfer = []
#        
#        
#    def attribute_exists(self, attribute):
#        if self.__dict__.has_key(attribute):
#            return True
#        else:
#            return False
#        
#    
#    def list_attributes(self):
#        return self.__dict__
#    
#    
#    def attribute_is_vector(self, attribute):
#        return isinstance(attribute, list)
#    
#    
#    def get_attribute(self, attribute):
#        if self.__dict__.has_key(attribute):
#            return self.__dict__[attribute]
#        return None
#    