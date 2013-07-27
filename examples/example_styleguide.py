import os
import sys
import pilot
import traceback

""" DESCRIPTION: This example does this...
"""

# Redis password is read from the environment. The example can be run like this:
# REDIS_PASSWORD=ILikeBigJob_wITH-REdIS python examples/example_styleguide.py
# Alternatively, for tutorials, etc. REDIS_PASSWORD can be defined in /etc/profile

#------------------------------------------------------------------------------
# 
REDIS_PWD = os.environ.get('REDIS_PASSWORD')
COORD     = "redis://%s@gw68.quarry.iu.teragrid.org:6379" % REDIS_PWD
HOST      = "ssh://localhost"

#------------------------------------------------------------------------------
#
def main():
    try:
        pilot_description = pilot.PilotComputeDescription()
        pilot_description.service_url = HOST
        pilot_description.number_of_processes = 1
        pilot_description.working_directory = os.getcwd()

        pilot_service = pilot.PilotComputeService(COORD)

        ### This is broken !!! -> https://github.com/saga-project/BigJob/issues/118
        #pilotjob = pilot_service.create_pilot(pilot_compute_description)
        pilotjob = pilot_service.create_pilot(pilot_compute_description=pilot_description)

        task = pilot.ComputeUnitDescription()
        task.executable = "/bin/sleep"
        task.arguments = ["10"]

        pilotjob.submit_compute_unit(task)

        # do something useful here, wait or whatever. print some information.


        # Not sure how to cancel properly
        #   - see https://github.com/saga-project/BigJob/issues/121
        #   - see https://github.com/saga-project/BigJob/issues/131
        for i, pj in enumerate(pjs):
            print "cancel %3d" % i
            pj.cancel()

        pilot_service.cancel()

        return(0)

    except Exception, ex:
            print "AN ERROR OCCURED: %s" % ((str(ex)))
            # print a stack trace in case of an exception -
            # this can be helpful for debugging the problem
            traceback.print_exc()
            return(-1)

if __name__ == "__main__":
    sys.exit(main())
