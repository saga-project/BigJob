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
SAGA_BLISS=False
try:
    import ConfigParser
    _CONFIG_FILE="bigjob.conf"
    _conf_file = os.path.dirname(os.path.abspath( __file__ )) + "/../" + _CONFIG_FILE
    _config = ConfigParser.ConfigParser()
    logger.debug ("read configfile: " + _conf_file)
    _config.read(_conf_file)
    default_dict = _config.defaults()        
    saga = default_dict["saga"]

    if saga.lower() == "bliss":
        SAGA_BLISS=True    
except:
    logger.error("bjgjob.conf could not be read") 

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
