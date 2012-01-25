#!/usr/bin/python
import sys
import os
import urllib
import time

start_time = time.time()
home = os.getcwd()
BIGJOB_AGENT_DIR= os.path.join(home, ".bigjob")
if not os.path.exists(BIGJOB_AGENT_DIR):
    os.mkdir (BIGJOB_AGENT_DIR)
BIGJOB_PYTHON_DIR=BIGJOB_AGENT_DIR+"/python/"
BOOTSTRAP_URL="https://raw.github.com/drelu/BigJob/master/bootstrap/bigjob-bootstrap.py"
BOOTSTRAP_FILE=BIGJOB_AGENT_DIR+"/bigjob-bootstrap.py"
#ensure that BJ in .bigjob is upfront in sys.path
sys.path.insert(0, os.getcwd() + "/../")
sys.path.insert(0, os.getcwd() + "/../../")
p = list()
for i in sys.path:
    if i.find(".bigjob/python")>1:
          p.insert(0, i)
for i in p:
    sys.path.insert(0, i)
    print str(sys.path)
try:
    import saga
except:
    print "SAGA and SAGA Python Bindings not found: BigJob only work w/ non-SAGA backends e.g. Redis, ZMQ.";print "Python version: ",  os.system("python -V");print "Python path: " + str(sys.path)
try:
    import bigjob.bigjob_agent
except:
    print "BigJob not installed. Attempting to install it."; opener = urllib.FancyURLopener({}); opener.retrieve(BOOTSTRAP_URL, BOOTSTRAP_FILE); os.system("python " + BOOTSTRAP_FILE + " " + BIGJOB_PYTHON_DIR); activate_this = BIGJOB_PYTHON_DIR+'bin/activate_this.py'; execfile(activate_this, dict(__file__=activate_this))
#try to import BJ once again
import bigjob.bigjob_agent_condor
# execute bj agent

args = list()
args.append("bigjob_agent_condor.py")
args.append(sys.argv[1])
args.append(sys.argv[2])

print "Bootstrap time: " + str(time.time()-start_time)
print "Starting BigJob Agents with following args: " + str(args)
#bigjob_agent = bigjob.bigjob_agent.bigjob_agent(args)
bigjob_agent = bigjob.bigjob_agent_condor.bigjob_agent(args)