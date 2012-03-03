#!/usr/bin/env python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, Ole Christian Weidner"
__license__   = "MIT"

import time
from bigjob_osg import bigjob, subjob, description, state

def bigjob_osg_example_simple():
    lrms_url = "condor://localhost"

    print "Start Pilot Job/BigJob at: " + lrms_url
    bj = bigjob(None)

    ##############################
    # Start the pilot job 
    bj.start_pilot_job(lrms_url)
    print "Pilot Job/BigJob URL: " + bj.pilot_url + " State: " + str(bj.get_state())

    ##############################
    # Define a workload
    jd = description()
    jd.set_attribute        ("Executable", "/home/oweidner/software/bfast/bin/bfast")
    jd.set_vector_attribute ("Arguments", ["match", "-r", "reads.10.fastq", "-A", "1", "-f", "hg_2122.fa"])
    jd.set_attribute        ("Output", "my_out.$(Cluster).$(Process)")
    jd.set_attribute        ("Error", "my_err.$(Cluster).$(Process)")
    
    input_files = ["/home/oweidner/software/bfast/data/small/reference/hg_2122.fa > hg_2122.fa",
      "/home/oweidner/software/bfast/data/small/reference/hg_2122.fa.cs.10.1.bif > hg_2122.fa.cs.10.1.bif",
      "/home/oweidner/software/bfast/data/small/reference/hg_2122.fa.cs.9.1.bif > hg_2122.fa.cs.9.1.bif",
      "/home/oweidner/software/bfast/data/small/reference/hg_2122.fa.cs.8.1.bif > hg_2122.fa.cs.8.1.bif",
      "/home/oweidner/software/bfast/data/small/reference/hg_2122.fa.cs.7.1.bif > hg_2122.fa.cs.7.1.bif",
      "/home/oweidner/software/bfast/data/small/reference/hg_2122.fa.cs.6.1.bif > hg_2122.fa.cs.6.1.bif",
      "/home/oweidner/software/bfast/data/small/reference/hg_2122.fa.cs.5.1.bif > hg_2122.fa.cs.5.1.bif",
      "/home/oweidner/software/bfast/data/small/reference/hg_2122.fa.cs.4.1.bif > hg_2122.fa.cs.4.1.bif",
      "/home/oweidner/software/bfast/data/small/reference/hg_2122.fa.cs.3.1.bif > hg_2122.fa.cs.3.1.bif",
      "/home/oweidner/software/bfast/data/small/reference/hg_2122.fa.cs.2.1.bif > hg_2122.fa.cs.2.1.bif",
      "/home/oweidner/software/bfast/data/small/reference/hg_2122.fa.cs.1.1.bif > hg_2122.fa.cs.1.1.bif",
      "/home/oweidner/software/bfast/data/small/reference/hg_2122.fa.cs.brg > hg_2122.fa.cs.brg",
      "/home/oweidner/software/bfast/data/small/reference/hg_2122.fa.nt.brg > hg_2122.fa.nt.brg",
      "/home/oweidner/software/bfast/data/small/reads_5K/reads.10.fastq > reads.10.fastq"]
    jd.set_vector_attribute("FileTransfer", input_files)

    ##############################
    # Submit the workload to the pilot job
    sj = subjob()
    sj.submit_job(bj, jd)
    while 1:
        state = str(sj.get_state())
        print "Subjob state: " + state
        if(state=="Failed" or state=="Done"):
            break
        time.sleep(5)

    ##############################
    # Stop the pilot job 
    bj.cancel()
    print "Pilot Job/BigJob URL: " + bj.pilot_url + " State: " + str(bj.get_state())

if __name__ == "__main__":
    bigjob_osg_example_simple()
