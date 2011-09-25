#!/usr/bin/python
import sys
import os
import urllib
import sys
import time
import optparse

parser = optparse.OptionParser()    
parser.add_option("-a", "--arg1", dest="arg1", help="")
parser.add_option("-b", "--arg2", dest="arg2", help="")

(options, args) = parser.parse_args()
if not (options.arg1) or not options.arg2 :
   #sys.exit(0)  
   pass
start_time = time.time()

home = os.environ["HOME"]

BIGJOB_AGENT_DIR= home+ "/.bigjob5"
if not os.path.exists(BIGJOB_AGENT_DIR):
    os.mkdir (BIGJOB_AGENT_DIR)
BIGJOB_PYTHON_DIR=BIGJOB_AGENT_DIR+"/python/"
BOOTSTRAP_URL="https://svn.cct.lsu.edu/repos/saga-projects/applications/bigjob/trunk/generic/bootstrap/bigjob-bootstrap.py"
BOOTSTRAP_FILE=BIGJOB_AGENT_DIR+"/bigjob-bootstrap.py"

"""
try: import saga
except: print "SAGA and SAGA Python Bindings not found: BigJob only work w/ non-SAGA backends (e.g. Redis, ZMQ).";print "Python version: ",  os.system("python -V");print "Python path: " + str(sys.path)
"""   
sys.path.insert(0, os.getcwd() + "/../")
sys.path.insert(0, os.getcwd() + "/../../")
    
try: import bigjob.bigjob_agent
except: print "BigJob not installed. Attempting to install it."; opener = urllib.FancyURLopener({}); opener.retrieve(BOOTSTRAP_URL, BOOTSTRAP_FILE); os.system("/usr/bin/python " + BOOTSTRAP_FILE + " " + BIGJOB_PYTHON_DIR); activate_this = BIGJOB_PYTHON_DIR+'bin/activate_this.py'; execfile(activate_this, dict(__file__=activate_this))

#try to import BJ once again
import bigjob.bigjob_agent
    
# execute bj agent
args = ["bigjob_agent.py", options.arg1 , options.arg2]
#args = ["bigjob_agent.py", "aa" , "ed"]
print "Bootstrap time: " + str(time.time()-start_time)
print "Starting BigJob Agents with following args: " + str(args)
bigjob_agent = bigjob.bigjob_agent.bigjob_agent(args)           
