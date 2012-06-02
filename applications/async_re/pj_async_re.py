# File Based Replica Exchange class
"""A module to prepare and run file-based asynchronous RE jobs

Contributors: 

Emilio Gallicchio <emilio.gallicchio@rutgers.edu>


Approach:

1. A set of subjobs (replicas) are set up by using MD engine input
   files derived from template input files and a list of thermodynamic
   settings which distinguish one replica from another. Template input
   files are assumed to exist in the working directory. Each replica
   is set up into its own sub-directory of the working directory named
   r0, r1, ..., r<M-1>, where M is the number of replicas.

2. Periodically a subset of the replicas is launched on remote
   execution hosts specified by a nodefile and enter a "R" (running)
   state. When a replica completes a run (of length specified in the
   MD engine input file), referred to in the code as a "cycle", it
   enters a "W" (wait) state which makes it eligible for exchange of
   thermodynamic parameters with other replicas and the initiation of
   a new run cycle.

3. Periodically exchanges of thermodynamic parameters are attempted
   between randomly picked pairs of replicas in the wait state based
   on the appropriate replica exchange rules, which are implemented
   based on their thermodynamic settings (temperature, for example)
   and their current structural and energetic information obtained
   from the MD engine output files.

Execution:

#!/bin/bash
ASYNCRE_DIR=<directory where async_re.py is installed>
export PYTHONPATH=$ASYNCRE_DIR:$ASYNCRE_DIR/configobj-4.7.2:$PYTHONPATH
python $ASYNCRE_DIR/async_re.py command_file.inp > LOG 2>&1 &

where 'command_file.inp' is a keyword=value input file described
below.  Execution terminates after a specified amount of wall-clock
time (see below). Internal state is saved periodically and at the end
of execution so that is can be restarted at a later time (see below)

Module requirements: 

pickle, ConfigObj 

Command file:

The command file file lists "keyword=value" items. For example:

NODEFILE = 'nodefile' 
walltime = 300
ENGINE = 'IMPACT'
RE_TYPE = 'BEDAM'
RE_SETUP = 'yes'
ENGINE_INPUT_BASENAME = 'hg'
ENGINE_INPUT_EXTFILES = 'heptanoate_rcpt_restr.maegz,heptanoate.maegz,heptanoate_cmrestraint.dat,hg_0.rst,agbnp2.param'
LAMBDAS = '0.04,0.07,0.1,0.25,0.35,0.45,0.55,0.65,0.71,0.78,0.85,1.0'
BEDAM_TEMPERATURE = 300.0

  NODEFILE: (required) a list of remote execution hosts one per
  line. Pass-wordless ssh required. Also assumes a uniform filesystem
  on the local host and the remote execution hosts.

  walltime: (required) execution time in seconds. After walltime seconds the
  application exits. Launched jobs will continue to run to completion.

  ENGINE: (required) MD engine program. Only 'IMPACT' is currently
  supported.

  RE_TYPE: (required) Replica exchange application. Only 'BEDAM'
  (Hamiltonian RE for binding free energy calculations) is currently
  supported.

  RE_SETUP: (defaults to 'no') whether to setup a new RE
  simulation. 'no' is used to restart a previously interrupted RE job.

  ENGINE_INPUT_BASENAME: (required) basename of the job, used to
  locate the engine input file and associated files (for IMPACT the
  input file is BASENAME.inp), and to write the status files
  (BASENAME.stat and BASENAME_stat.txt).

  ENGINE_INPUT_EXTFILES: list of structure files etc. that are copied
  from working directory to the replicas directories to start each
  replica.

  LAMBDAS, BEDAM_TEMPERATURE: BEDAM-specific settings, there's one
  element in LAMBDA for each replica.  For other applications some
  other quantity (such as TEMPERATURE) would distinguish one replica
  from another.

Constructor:

  rx = async_re_job(commandFile, options=None)

Public functions:

setupJob()
updateStatus()
print_status()
launchJobs()
doExchanges()

Functions that need to be specialized for each supported MD engine/RE
application combination:

_buildInpFile()
_isDone()
_launchReplica()
_doExchange_pair()

all the other methods apply generically to all engine/application types.

"""

import os, sys, time, re, pickle, random, math, time
import pdb
import shutil, signal, glob
from configobj import ConfigObj
from pilot import PilotComputeService, ComputeDataService, State


class async_re_job:
    """
    Class to set up and run asynchronous file-based RE calculations
    """
    def __init__(self, command_file, options):
        self.command_file = command_file
        self.cus={}
        self.jobname = os.path.splitext( os.path.basename(command_file) )[0]

        self._parseInputFile()

        self._checkInput()

        self._printStatus()


    def _error(self, text):
        """ Print an error line to the log file """
        print text
        sys.stdout.flush()
    
    def _exit(self, text):
        """ Print an error and exit """
        self._error(text)
        print 'exiting...'
        sys.exit(1)

    def _parseInputFile(self):
        """
        Read keywords from control file. Requires the ConfigObj module
        """
        self.keywords = ConfigObj(self.command_file)
        print self.keywords
        

    def _printStatus(self):
        """ Logs input parameters """
        print 'command_file=', self.command_file
        print 'jobname=', self.jobname
        for k, v in self.keywords.iteritems():
            print k, v

    def _checkInput(self):
        """ 
Checks that the required parameters are specified and parses
these and the other settings.
"""
        #required options
        if self.keywords.get('RE_TYPE') is None:
            self._exit("RE_TYPE = [BEDAM|DATE] needs to be specified")
            
        if self.keywords.get('COORDINATION_URL') is None:
            self._exit("COORDINATION_URL needs to be specified")
        if self.keywords.get('RESOURCE_URL') is None:
            self._exit("RESOURCE_URL needs to be specified")
        if self.keywords.get('QUEUE') is None:
            self._exit("QUEUE needs to be specified")
        if self.keywords.get('BJ_WORKING_DIR') is None:
            self._exit("BJ_WORKING_DIR needs to be specified")
        if self.keywords.get('TOTAL_CORES') is None:
            self._exit("TOTAL_CORES needs to be specified")
        if self.keywords.get('PPN') is None:
            self._exit("PPN needs to be specified")
        if self.keywords.get('SPMD') is None:
            self.spmd="single"
        else:
            self.spmd="mpi"    
        if self.keywords.get('SUBJOB_CORES') is None:
            self._exit("SUBJOB_CORES needs to be specified")
                    
        if self.keywords.get('ENGINE') is None:
            self._exit("ENGINE = [IMPACT|DATEENGINE] needs to be specified")
        if self.keywords.get('ENGINE_INPUT_BASENAME') is None:
            self._exit("ENGINE_INPUT_BASENAME needs to be specified")
        if self.keywords.get('WALL_TIME') is None:
            self._exit("WALL_TIME needs to be specified")

        self.basename = self.keywords.get('ENGINE_INPUT_BASENAME')
        self.walltime = float(self.keywords.get('WALL_TIME'))

        if self.keywords.get('RE_TYPE') == 'BEDAM' or self.keywords.get('RE_TYPE') == 'DATE':
            if self.keywords.get('LAMBDAS') is None:
                 self._exit("LAMBDAS needs to be specified")
            self.lambdas = self.keywords.get('LAMBDAS').split(',')
            self.nreplicas = len(self.lambdas)
            if self.keywords.get('BEDAM_TEMPERATURE') is None:
                self._exit("BEDAM_TEMPERATURE is a required parameter")
            bedam_temperature = float(self.keywords.get('BEDAM_TEMPERATURE'))
            self.bedam_beta = 1./(0.0019872041*bedam_temperature)

        #if self.keywords.get('ENGINE') == 'IMPACT':
        #    self.impact = os.path.join(os.environ['SCHRODINGER'],'impact')

        if not (self.keywords.get('ENGINE_INPUT_EXTFILES') is None): 
            self.extfiles = self.keywords.get('ENGINE_INPUT_EXTFILES')
            if self.extfiles != '':
                self.extfiles = self.extfiles.split(',')

        self.status_file = "%s.stat" % self.basename


    def setupJob(self):
        """
If RE_SETUP='yes' creates and populates subdirectories, one for each replica 
called r0, r1, ..., rN in the working directory. Otherwise reads saved state
from the ENGINE_BASENAME.stat file.

To populate each directory calls _buildInpFile(k) to prepare the MD engine
input file for replica k. Also creates soft links to the working directory 
for the accessory files specified in ENGINE_INPUT_EXTFILES.
"""
        if not (self.keywords.get('RE_SETUP') is None) and (self.keywords.get('RE_SETUP') == "yes"):
            # create replicas directories r1, r2, etc.
            for k in range(self.nreplicas):
                os.mkdir("r%d" % k)
            # create links for external files
            if self.extfiles != None:
                for k in range(self.nreplicas):
                    os.chdir("r%d" % k)
                    for file in self.extfiles:
                        if os.path.exists("../%s" % file) != True:
                            msg = "No such file: %s" % file
                            self._exit(msg)
                        os.symlink("../%s" % file, file)
                    os.chdir("..")
            # create status table
            self.status = []
            for k in range(self.nreplicas):
                st = {}
                st['stateid_current'] = k
                st['running_status'] = "W"
                st['cycle_current'] = 1
                ##st['compute_node'] = None
                self.status.append(st)
            #initializes compute nodes running status
            #self.node_status = []
            #for n in range(self.nprocs):
                #when running something it would have the id of the
                #replica it's running
                #self.node_status.append(None)
            # save status tables
            self._write_status()
            # create input files no. 1
            for k in range(self.nreplicas):
                self._buildInpFile(k)
        else:
            self._read_status()

        self.updateStatus()
        self.print_status()
        self.pj = PilotComputeService(self.keywords.get('COORDINATION_URL'))
        self.cds=ComputeDataService()
        self.launch_pilotjob()

    def cleanJob(self):
        self.cds.cancel()
        self.pj.cancel()
    def waitJob(self):
        self.cds.wait()

        
    def launch_pilotjob(self):
        pcd={"service_url":self.keywords.get('RESOURCE_URL'),
             "number_of_processes":self.keywords.get('TOTAL_CORES'),
             "working_directory": self.keywords.get('BJ_WORKING_DIR'),
             "queue":self.keywords.get('QUEUE'),
             "processes_per_node":self.keywords.get('PPN'),
	     "allocation":self.keywords.get('PROJECT'),
             "walltime":int(self.keywords.get('WALL_TIME'))/60}
         
        self.pj.create_pilot(pilot_compute_description=pcd)
        self.cds.add_pilot_compute_service(self.pj)
        
            
    def _write_status(self):
        """
Saves the current state of the RE job in the BASENAME.stat 
file using pickle
"""
        f = open(self.status_file, "w")
        pickle.dump(self.status, f)
        #pickle.dump(self.node_status, f)
        f.close()

    def _read_status(self):
        """
Loads the current state of the RE job from BASENAME.stat 
file using pickle
"""
        f = open(self.status_file, "r")
        self.status = pickle.load(f)
        f.close()

    def print_status(self):
        """
Writes to BASENAME_stat.txt a text version of the status of the RE job. 

It's fun to follow the progress in real time by doing:

watch cat BASENAME_stat.txt
"""
        logfile = "%s_stat.txt" % self.basename
        ofile = open(logfile,"w")
        log = "Replica  State  Status  Cycle \n"
        for k in range(self.nreplicas):
            log += "%6d   %5d  %5s  %5d \n" % (k, self.status[k]['stateid_current'], 
                self.status[k]['running_status'], self.status[k]['cycle_current'])
        log += "Running = %d\n" % self.running
        log += "Waiting = %d\n" % self.waiting

        ofile.write(log)
        ofile.close()
        
    
    
         
    def _buildInpFile(self, replica):
        """
Generic function to prepare the input file for the MD engine for the specified
replica for the current cycle and thermodynamic state. Calls specialized 
functions depending on RE_TYPE and ENGINE.
"""
        if self.keywords.get('RE_TYPE') == 'BEDAM':
            self._buildInpFile_BEDAM(self.basename, replica, self.status[replica]['stateid_current'], 
                                    self.status[replica]['cycle_current'])

    def _buildInpFile_BEDAM(self, basename, replica, stateid, cycle):
        """
Builds input file for a BEDAM replica based on template input file
BASENAME.inp for the specified replica at lambda=lambda[stateid] for the
specified cycle.
"""
        template = "%s.inp" % basename
        inpfile = "r%d/%s_%d.inp" % (replica, basename, cycle)
        q = 3
        lambd = self.lambdas[stateid]
        # read template buffer
        tfile = open(template, "r")
        tbuffer = tfile.read()
        tfile.close()
        # make modifications
        tbuffer = tbuffer.replace("@n@",str(cycle))
        tbuffer = tbuffer.replace("@nm1@",str(cycle-1))
        tbuffer = tbuffer.replace("@lambda@",lambd)
        # write out
        ofile = open(inpfile, "w")
        ofile.write(tbuffer)
        ofile.close()

    def updateStatus(self):
        """
Scans replica to update their state. 
"""
        for k in range(self.nreplicas):
            self._updateStatus_replica(k)
        self._write_status()
        self._update_running_no()

    def _updateStatus_replica(self,replica):
        """
Update the status of the specified replica. If it has completed a cycle the 
input file for the next cycle is prepared and the replica is placed in
the wait state.
"""
        if self.status[replica]['running_status'] == "R":
            if self._isDone(replica,self.status[replica]['cycle_current']):
                self.status[replica]['running_status'] = "S"
                self.status[replica]['cycle_current'] += 1
                self._buildInpFile(replica)
                self.status[replica]['running_status'] = "W"
                #node = self.status[replica]['compute_node']
                #self.node_status[node] = None
                #self.status[replica]['compute_node'] = None
  
    def _update_running_no(self):
        """
Updates the number of running replicas
"""
        self.running = 0
        self.waiting = 0
        for k in range(self.nreplicas):
            if self.status[k]['running_status'] == "R":
                self.running += 1
            if self.status[k]['running_status'] == "W":
                self.waiting += 1

    def _isDone(self,replica,cycle):
        """
Generic function to check if a replica completed a cycle. Calls specialized
functions depending on ENGINE.
"""
        if self.keywords.get('ENGINE') == 'IMPACT':
            return self._isDone_IMPACT(replica,cycle)
        if self.keywords.get('ENGINE') == 'DATE':
            return self._isDone_DATE(replica,cycle)

    def _isDone_IMPACT(self,replica,cycle):
        """
Returns true if an IMPACT replica has completed a cycle. Basically checks
if the restart file exists.
"""
        rstfile = "r%d/%s_%d.rst" % (replica, self.basename,cycle)
        if os.path.exists(rstfile):
            return True
        else:
            return False


    def _isDone_DATE(self,replica,cycle):
        """
Returns true if an IMPACT replica has completed a cycle. Basically checks
if the restart file exists.
"""
        #rstfile = "r%d/%s_%d.rst" % (replica, self.basename,cycle)
        
        if self.cus[replica].get_state() == "Done":
            return True
        else:
            return False
            
        
    def launchJobs(self):
        """
Scans the replicas in wait state and randomly launches some of them
if CPU's are available.
""" 
        # find out the replicas waiting
        self._update_running_no()
        #if self.waiting > 0 and self.running < self.nprocs:
        if self.waiting > 0:
            wait = []
            for k in range(self.nreplicas):
                if self.status[k]['running_status'] == "W":
                    wait.append(k)
            for k in wait:
                self._launchReplica(k,self.status[k]['cycle_current'])
                
    def _launchReplica(self,replica,cycle):
         """
Performs the actual work of launching a replica. Calls specialized functions
based on ENGINE.
"""
         if self.keywords.get('ENGINE') == 'IMPACT':
             self._launchReplica_IMPACT(replica,cycle,nodename)
         if self.keywords.get('ENGINE') == 'DATE':
             self._launchReplica_DATE(replica,cycle)        
         self.status[replica]['running_status'] = "R"
         
    def _launchReplica_IMPACT(self,replica,cycle,nodename):
         """
Issues a command via ssh to launch Impact on a remote host.
"""
         num_threads = os.getenv('OMP_NUM_THREADS')
         if num_threads == None:
             num_threads = 1
         else:
             num_threads = int(num_threads)
         launch_command = "cd %s ; export OMP_NUM_THREADS=%d ; export SCHRODINGER=%s ; %s -i %s_%d.inp -e main1m.e2 -LOCAL" % (os.getcwd(), num_threads, os.environ['SCHRODINGER'], self.impact, 
                 self.basename, cycle)
         print "ssh %s '%s'" % (nodename,launch_command)
         os.system("ssh %s '%s'" % (nodename,launch_command))

    def _launchReplica_DATE(self,replica,cycle):
        """
Issues a command via ssh to launch Impact on a remote host.
"""
        compute_unit_description = {
            "executable": "/bin/date",
            "arguments": [""],
            "total_cpu_count": int(self.keywords.get('SUBJOB_CORES')),            
            "output": "sj-stdout-"+str(replica)+".txt",
            "error": "sj-stderr-"+str(replica)+".txt",   
            "working_directory":os.getcwd()+"/r"+str(replica),
            "spmd_variation":self.keywords.get('SPMD')
        }  
        compute_unit=self.cds.submit_compute_unit(compute_unit_description)
        self.cus[replica]=compute_unit
        
         
    def doExchanges(self):
        """
Randomly selects a pair of replicas in wait state for exchange of 
thermodynamic parameters. 
"""
        # find out which replicas are waiting
        self._update_running_no()
        if self.waiting > 1:
            replicas_waiting = []
            for k in range(self.nreplicas):
                if self.status[k]['running_status'] == "W" and self.status[k]['cycle_current'] > 1:
                    replicas_waiting.append(k)
            random.shuffle(replicas_waiting)
            for k in range(0,len(replicas_waiting)-1,2):
                self._doExchange_pair(replicas_waiting[k],replicas_waiting[k+1])

    def _doExchange_pair(self,repl_a,repl_b):
        """
Generic function to perform an exchange between replicas repl_a and repl_b.

1. Places the two replicas in "E" (exchanging state)
2. rewinds the cycle to the previous completed cycle
3. performs the exchange by calling specialized 
   functions based on ENGINE and RE_TYPE.
4. Creates new input file for the next cycle
5. Places replicas back into "W" (wait) state 
"""
        self.status[repl_a]['running_status'] = "E"
        self.status[repl_b]['running_status'] = "E"
        self.status[repl_a]['cycle_current'] -= 1
        self.status[repl_b]['cycle_current'] -= 1
        if self.keywords.get('RE_TYPE') == 'BEDAM':
            self._doExchange_pair_BEDAM(repl_a,repl_b)
        self.status[repl_a]['cycle_current'] += 1
        self.status[repl_b]['cycle_current'] += 1
        self._buildInpFile(repl_a)
        self._buildInpFile(repl_b)
        self.status[repl_a]['running_status'] = "W"
        self.status[repl_b]['running_status'] = "W" 

    def _doExchange_pair_BEDAM(self,repl_a,repl_b):
        """
Performs exchange of lambdas for BEDAM replica exchange.        
"""
        cycle_a = self.status[repl_a]['cycle_current']
        sid_a = self.status[repl_a]['stateid_current']
        lambda_a = self.lambdas[sid_a]
        u_a = self._extractLast_BindingEnergy(repl_a,cycle_a)

        cycle_b = self.status[repl_b]['cycle_current'] 
        sid_b = self.status[repl_b]['stateid_current']
        lambda_b = self.lambdas[sid_b]
        u_b = self._extractLast_BindingEnergy(repl_b,cycle_b)

        dl = float(lambda_b) - float(lambda_a)
        du = float(u_b) - float(u_a)
        delta = -dl*du

        print "Pair Info"
        print "%d %s %s" % (repl_a, lambda_a, u_a)
        print "%d %s %s" % (repl_b, lambda_b, u_b)
        print "dl = %f du = %f delta = %f" % (dl,du,delta)

        csi = random.random()
        if math.exp(-self.bedam_beta*delta) > csi:
            print "Accepted %f %f" % (math.exp(-self.bedam_beta*delta),csi)
            print (self.status[repl_a]['stateid_current'], self.status[repl_b]['stateid_current'])
            self.status[repl_a]['stateid_current'] = sid_b
            self.status[repl_b]['stateid_current'] = sid_a
            print (self.status[repl_a]['stateid_current'], self.status[repl_b]['stateid_current'])
        else:
            print "Rejected %f %f" % (math.exp(-self.bedam_beta*delta),csi)

    def _extractLast_BindingEnergy(self,repl,cycle):
        """
Extracts binding energy from Impact output
"""
        output_file = "r%s/hg_%d.out" % (repl,cycle)
        datai = self._getImpactData(output_file)
        nf = len(datai[0])
        nr = len(datai)
        return datai[nr-1][nf-1]

    def _getImpactData(self, file):
        """
Reads all of the Impact simulation data values temerature, energies, etc.
at each time step and puts into a big table
"""
        if not os.path.exists(file):
            msg = 'File does not exist: %s' % file
            self._exit(msg)
        step_line = re.compile("^ Step number:")
        number_line = re.compile("(\s+-*\d\.\d+E[\+-]\d+\s*)+")
        nsamples = 0
        data = []
        f = open(file ,"r")
        line = f.readline()
        while line:
            # fast forward until we get to the line: 
            # "Step number: ... "
            while line and not re.match(step_line, line): 
                line = f.readline()
            # read the step number
            if re.match(step_line, line):
                words = line.split()
                step = words[2]
                #now read up to 3 lines of numbers
                datablock = [int(step)]
                ln = 0
                while ln < 3:
                    line = f.readline()
                    if not line:
                        msg = "Unexpected end of file"
                        self._exit(msg)
                    if re.match(number_line, line):
                        for word in line.split():
                            datablock.append(float(word))
                        ln += 1
                data.append(datablock)
            line = f.readline()
        f.close()
        return data


if __name__ == '__main__':

    # Parse arguments:
    usage = "%prog <ConfigFile>"
    
    if len(sys.argv) != 2:
        print "Please specify ONE input file"
        sys.exit(1)
    
    commandFile = sys.argv[1]

    print ""
    print "===================================="
    print "   Asynchronous Replica Exchange    "
    print "===================================="
    print ""
    print "Started at: " + str(time.asctime())
    print "Input file:", commandFile
    print ""
    sys.stdout.flush()
    
    print "Read control file"

    rx = async_re_job(commandFile, options=None)

    rx.setupJob()
    start_time = time.time()
    
    #while time.time() < start_time + rx.walltime:
    ## NBR_EXCHANGES for temporary
    xcg=1
    NBR_EXCHANGES=5
    while xcg <= NBR_EXCHANGES:
        rx.updateStatus()
        rx.print_status()
    
        time.sleep(1)
        rx.launchJobs()
        rx.updateStatus()
        rx.print_status()
        
        time.sleep(1)
        
        rx.doExchanges()
        
        time.sleep(1)
        xcg+=1

    rx.waitJob()
    rx.cleanJob()



