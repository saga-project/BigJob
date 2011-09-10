import sys
import os
import urllib

home = os.environ['HOME']

BIGJOB_AGENT_DIR= home+ "/.bigjob"
BIGJOB_PYTHON_DIR=BIGJOB_AGENT_DIR+"/python/"
BOOTSTRAP_URL="http://localhost/~luckow/bigjob-bootstrap.py"
BOOTSTRAP_FILE=BIGJOB_AGENT_DIR+"/bigjob-bootstrap.py"

try:
    import saga
except:
    print "SAGA and SAGA Python Bindings not found: Please install SAGA first (http://saga.cct.lsu.edu)."
    sys.exit(1)
    
try:
    import bigjob.bigjob_agent
except:
    print "BigJob not installed. Attempting to install it."
    opener = urllib.FancyURLopener({})
    opener.retrieve(BOOTSTRAP_URL, BOOTSTRAP_FILE)
    os.system("python " + BOOTSTRAP_FILE + " " + BIGJOB_PYTHON_DIR)


activate_this = BIGJOB_PYTHON_DIR+'bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))
    
# execute bj agent
args = [sys.argv[0], "advert://localhost", "advert://localhost"]
bigjob_agent = bigjob.bigjob_agent(args)    