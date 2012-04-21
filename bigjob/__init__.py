import os
import logging

version = "latest"

#READ config
SAGA_BLISS=False
try:
    import ConfigParser
    _CONFIG_FILE="bigjob.conf"
    _conf_file = os.path.dirname(os.path.abspath( __file__ )) + "/../" + _CONFIG_FILE
    _config = ConfigParser.ConfigParser()
    _config.read(_conf_file)
    default_dict = _config.defaults()
    
    level = default_dict["logging.level"]
    print("Logging level: %s"%level) 
    if level.startswith("logging."):
        logging_level = eval(level)        
        print("Set logging level: %s/%s"%(level,logging_level))
        logging.basicConfig(level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p',
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger = logging.getLogger(name='bigjob')
        logger.setLevel(logging_level)
    
    else:
        logging.basicConfig(level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p',
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')    
        logger = logging.getLogger(name='bigjob')
            
      
    saga = default_dict["saga"]
    if saga.lower() == "bliss":
        SAGA_BLISS=True    
        
except:
    print("bjgjob.conf could not be read") 
    
import socket
try:
    fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", 'VERSION')
    version = open(fn).read().strip()
    logger.info("Loading BigJob version: " + version + " on " + socket.gethostname())
except IOError:
    pass



# define external-facing API
from bigjob.bigjob_manager import bigjob as myBigjob
from bigjob.bigjob_manager import subjob as mySubjob


class subjob(mySubjob):
    pass


class bigjob(myBigjob):
    pass


try:
    from bigjob.bigjob_manager import description as myDescription
    class description(myDescription):
        pass
except:
    pass
