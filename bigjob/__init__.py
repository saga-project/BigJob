import os
import pdb
import logging
logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                   format='%(asctime)s - %(levelname)s - %(message)s')

version = "latest"

try:
    fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", 'VERSION')
    version = open(fn).read().strip()
    logging.debug("Loading BigJob version: " + version)
except IOError:
    pass