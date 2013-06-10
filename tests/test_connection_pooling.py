import os
import time
import pilot

try:
    import pudb
    pudb.set_interrupt_handler()
except:
    pass

#########################################################################
##
redis_password = os.environ.get('REDIS_PASSWORD')
COORD    = "redis://%s@gw68.quarry.iu.teragrid.org:6379" % redis_password
HOST     = "ssh://localhost"
##
#########################################################################

N     = 20
pjs   = []
start = time.time()
total = 0.0

for i in range(0, N):

    print "start  %3d" % i

    pilot_description = pilot.PilotComputeDescription()
    pilot_description.service_url = HOST
    pilot_description.number_of_processes = 1
    pilot_description.working_directory = os.getcwd()

    pilot_service = pilot.PilotComputeService(COORD)

    ### This is broken !!! -> https://github.com/saga-project/BigJob/issues/118
    #pilotjob = pilot_service.create_pilot(pilot_compute_description)
    pilotjob = pilot_service.create_pilot(pilot_compute_description=pilot_description)

    pjs.append(pilotjob)

    task = pilot.ComputeUnitDescription()
    task.executable = "/bin/sleep"
    task.arguments = ["10"]

    pilotjob.submit_compute_unit(task)

stop = time.time()

# see https://github.com/saga-project/BigJob/issues/121
# see https://github.com/saga-project/BigJob/issues/131

for i, pj in enumerate(pjs):
    print "cancel %3d" % i
    pj.cancel()

pilot_service.cancel()


print "time: %.1fs   rate: %.1f/s" % (stop-start, N/(stop-start))
