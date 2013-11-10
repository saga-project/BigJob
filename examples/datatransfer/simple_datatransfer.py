"""As discussed in the CDI meeting on Nov. 7th, this examples shows how 'out-
of-band' (i.e., non-BigJob/PilotData) data transfer can be implemented using
SAGA-Pythons sftp file adaptor.

For this example Redis password is read from the environment. The example can 
be run like this::

    REDIS_PASSWORD=<put redis password here> python example_datatransfer.py

The application logic of this example is as follows:

    (1) We define the directory for our CU(s) explicitly as 
        WORKING_DIR + task name::

            task.working_directory = "%s/task-%s" % (WORKING_DIR, t)

      All task output ends up in that direcotry, including stdout and stderr.

    (2) We use SAGA-Python's SFTP file adaptor to retrieve the output files.
        SAGA-Python is installed together with BigJob, so it doesn't require
        any additional Python packages to be installed.

NOTE: If working directory is not defined, BigJob uses uuids to create unique
      directories for each bigjob and each tasks (bj-<uuid>/sj-<uuid>). 
      When setting working directory manually, you need to pay attention that 
      subsequent runs don't overwrite each others output files and directories. 
"""

import os
import sys
import saga
import pilot
import traceback

#------------------------------------------------------------------------------
# 
EXEC_HOST      = "login1.stampede.tacc.utexas.edu"
WORKING_DIR    = "/home1/00988/tg802352/mysim/"

REDIS_PASSWD   = os.environ.get('REDIS_PASSWORD')
REDIS_SERVER   = "redis://%s@gw68.quarry.iu.teragrid.org:6379" % REDIS_PASSWD

#------------------------------------------------------------------------------
#
def example_datatransfer():
    """Example entry point.
    """
    # we use this dictionary to store the paths of the 
    # individual tasks output directories::
    #
    #     {'task_id' : 'output_dir'}3
    #
    output_paths = {}

    ###################################
    # Step 1: Submit tasks via BigJob #
    ###################################
    try:
        pilot_description = pilot.PilotComputeDescription()
        pilot_description.service_url         = "slurm+ssh://"+EXEC_HOST
        pilot_description.number_of_processes = 16
        pilot_description.walltime            = 1
        pilot_description.project             = "TG-MCB090174"
        pilot_description.queue               = "normal"
        pilot_description.working_directory   = WORKING_DIR

        pilot_service = pilot.PilotComputeService(REDIS_SERVER)
        pilotjob = pilot_service.create_pilot(pilot_description)

        for t in range(0, 32):
            # Task output will end up in WORKING_DIR+/task-<t>+/stdout::
            # 
            #     /home1/00988/tg802352/mysim/task-<t>/stdout
            #
            output_path = "%s/task-%s" % (WORKING_DIR, t)
            output_paths[t] = output_path

            task = pilot.ComputeUnitDescription()
            task.executable        = "/bin/echo"
            task.working_directory = output_path
            task.arguments         = ["Hello from task %s" % t]
            pilotjob.submit_compute_unit(task)

        print "Waiting for tasks to finish..."
        pilotjob.wait()
        print "FINISHED"
        pilot_service.cancel()

    except Exception, ex:
        print "AN ERROR OCCURED: %s" % ((str(ex)))
        traceback.print_exc()
        return(-1)

    ###########################################
    # Step 2: Retrieve output files with SAGA #
    ###########################################
    try:
        print "Transferring output files back to local machine..."
        for task_id, output_path in output_paths.iteritems():
            remote_file = "sftp://%s/%s/stdout" % (EXEC_HOST, output_path)
            local_file  = "file://localhost/%s/task-%s.out" % (os.getcwd(), task_id)

            f = saga.filesystem.File(remote_file)
            f.copy(local_file)
            print "Copied %s -> %s" % (remote_file, local_file)

        print "FINISHED"

    except Exception, ex:
        print "AN ERROR OCCURED: %s" % ((str(ex)))
        traceback.print_exc()
        return(-1)

#------------------------------------------------------------------------------
#
if __name__ == "__main__":
    sys.exit(example_datatransfer())

