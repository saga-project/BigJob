BigJob implementation for OSG
=============================

This directory contains an implementation of the BigJob and Pilot APIs for use with the Open Science Grid (OSG) Condor pool. bigjob_osg is API compatible with the regular BigJob implementation, so it can be used with existing applications and experiments. However, its implementation and internal behavior is different.

Implementation
--------------

The <b>regular BigJob</b> encapsulates a resource reservation into a BigJob object which starts a pilot-job agent on a (remote) HPC cluster:

```
bj = bigjob(COORDINATION_URL)
    bj.start_pilot_job
```

The state of the reservation can be queried via: ```bj.get_state()``` and is initially ```Unknown``` and only switches to ```Running```, once the pilot-job agents are scheduled by the HPC queuing system.

<b>bigjob_osg</b> uses the same API calls to instantiate a bigjob pilot-job object, but instead of scheduling a pilot-job agent on an HPC system, it just connects to the local OSG Condor pool. OSG uses <i>glide-in WMS</i> to dynamically create a Condor glide-in (pilot-job) pool for HTC workloads, so it wouldn't be effective to try to overlay this pool with bigjob's own pilot-job agent mechanism. Instead, <b>bigjob_osg</b> internally handles the OSG Condor pool as a bigjob object with an <i>unlimited</i> amount of resources which is available (in ```Running``` state) as soon as it is instantiated.

Consequently, in the <bigjob_osg> implementation, subjobs that are added to a bigjob instance are passed on directly to the local condor pool.

Example
-------

While <b>bigjob_osg</b> is 100% API compatible with the existing, regular BigJob implementation, it doesn't require a lot of parameters, since it doesn't make use of pilot-job agents or a centralized communication and coordination mechanism. Non-relevant parameters will be silently ignored. 

This is an example of how to submit a single <i>BFAST</i> task to the OSG Condor pool via BigJob:

```
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
```