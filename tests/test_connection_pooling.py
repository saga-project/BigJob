import sys
import time
import bigjob

try :
    import pudb
    pudb.set_interrupt_handler()
except :
    pass


HOST  = "ssh://localhost"
COORD = "redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"

#HOST  = "ssh://boskop"
#COORD = "redis://10.0.1.18"

N     = 20
bjs   = []
start = time.time ()
total = 0.0


for i in range (0, N) :

    print "start  %3d" % i

    bj = bigjob.bigjob (COORD)
    bj.start_pilot_job (HOST)

    bjs.append (bj)

    jd = bigjob.description ()
    jd.executable          = "/bin/sleep"
    jd.arguments           = ["10"]
    
    sj = bigjob.subjob ()
    sj.submit_job (bj.pilot_url, jd)


stop = time.time ()


for i, bj in enumerate (bjs) :

    print "cancel %3d" % i
    bj.cancel ()


print "time: %.1fs   rate: %.1f/s" % (stop-start, N/(stop-start))


