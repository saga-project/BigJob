import os
import sys
import saga
import pilot
import traceback

""" This tutorial example extends and improves the first example
    (01_bigjob-simple-ensemble.py) by adding file transfer: once the tasks have
    finished executing, we use SAGA-Python to transfer the individual output
    files back to the local machine.
"""

# Redis password is read from the environment. The example can be run like this:
# REDIS_PASSWORD=ILikeBigJob_wITH-REdIS python examples/example_styleguide.py
# Alternatively, for tutorials, etc. REDIS_PASSWORD can be defined in /etc/profile

#------------------------------------------------------------------------------
#
REDIS_PWD   = os.environ.get('REDIS_PASSWORD')
# The coordination server
COORD       = "redis://%s@gw68.quarry.iu.teragrid.org:6379" % REDIS_PWD
# The host to run BigJob on
HOSTNAME    = "localhost"
# The working directory on the remote cluster / machine
WORKDIR     = os.getenv("HOME")+"/XSEDETutorial"
# The number of jobs you want to run
NUMBER_JOBS = 4


#------------------------------------------------------------------------------
#
def main():
    try:
        # this describes the parameters and requirements for our pilot job
        pilot_description = pilot.PilotComputeDescription()
        pilot_description.service_url = "ssh://%s" % HOSTNAME
        pilot_description.number_of_processes = 1
        pilot_description.working_directory = WORKDIR
        pilot_description.walltime = 10

        # create a new pilot job
        pilot_compute_service = pilot.PilotComputeService(COORD)
        pilotjob = pilot_compute_service.create_pilot(pilot_description)

        # submit tasks to pilot job
        task_set_A = list()
        for i in range(NUMBER_JOBS):
            task_desc = pilot.ComputeUnitDescription()
            task_desc.executable = '/bin/echo'
            task_desc.arguments = ['Hello', '$ENV1', "I am and 'A' task with CU number %s" % str(i)]
            task_desc.environment = {'ENV1': 'World'}
            task_desc.number_of_processes = 1
            task_desc.output = 'chained-example-A-stdout.txt'
            task_desc.error = 'chained-example-A-stderr.txt'
            task = pilotjob.submit_compute_unit(task_desc)
            print "* Submitted 'A' task '%s' with id '%s' to %s" % (i, task.get_id(), HOSTNAME)
            task_set_A.append(task)

        # Chaining tasks i.e submit a compute unit, when compute unit from A is successfully executed.
        # A 'B' task reads the content of the output file of an 'A' task and writes it into its own
        # output file.
        task_set_B = list()
        while len(task_set_A) > 0:
            for a_task in task_set_A:
                if a_task.get_state() == "Done":
                    print "* 'A' task %s finished. Launching a 'B' task." % (a_task.get_id())
                    task_desc = pilot.ComputeUnitDescription()
                    task_desc.executable = '/bin/cat'
                    task_desc.arguments = ["%s/chained-example-A-stdout.txt" % a_task.get_local_working_directory()]
                    task_desc.number_of_processes = 1
                    task_desc.output = 'chained-example-B-stdout.txt'
                    task_desc.error = 'chained-example-B-stderr.txt'
                    task = pilotjob.submit_compute_unit(task_desc)
                    print "* Submitted 'B' task '%s' with id '%s' to %s" % (i, task.get_id(), HOSTNAME)
                    task_set_B.append(task)
                    task_set_A.remove(a_task)

        print "Waiting for 'B' tasks to finish..."
        pilotjob.wait()

        # all 'B' tasks have finished. now we can use saga-python
        # to transfer back the output files...
        for task in task_set_B:
            d = saga.filesystem.Directory("sftp://%s/%s" % (HOSTNAME, task.get_local_working_directory()))
            local_filename = "stdout-%s.txt" % (task.get_id())
            d.copy("chained-example-B-stdout.txt", "file://localhost/%s/%s" % (os.getcwd(), local_filename))
            print "* Output for 'B' Task '%s' can be found locally in: './%s'" % (task.get_id(), local_filename)

        return(0)

    except Exception, ex:
            print "AN ERROR OCCURED: %s" % ((str(ex)))
            # print a stack trace in case of an exception -
            # this can be helpful for debugging the problem
            traceback.print_exc()
            return(-1)

    finally:
        # alway try to shut down pilots, otherwise jobs might end up
        # lingering in the queue
        print ("Terminating BigJob")
        pilotjob.cancel()
        pilot_compute_service.cancel()


if __name__ == "__main__":
    sys.exit(main())
