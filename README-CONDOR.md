Bigjob with OSG-Condorg
=============


Requirements:

	1) Account in OSG with access to Renci Gateway/portal machine(VO as Engage).
	2) Generate VOMS proxy on Renci gateway machine (similar to globus proxy.) with required certificates.
		
		-bash-3.2$ voms-proxy-init -voms Engage
		
** please refer to documentation on VOMS/OSG/Engage with provided urls during registration for more details.
		
		
Steps for running Bigjob on OSG's Condor-G

1) Install SAGA (https://svn.cct.lsu.edu/repos/saga-projects/extenci/osg_howto/HOWTO)

* SAGA >= 1.6
* SAGA-Condor Adaptor
* SAGA-Python Bindings 

2) Install Latest Bigjob

3) Edit SAGA Condor-G Adaptor ini file(saga_adaptor_condor_job.ini). 
   Typically it is located at $SAGA_LOCATION/share/saga/saga_adaptor_condor_job.ini

	[saga.adaptors.condor_job.default_attributes]
  	Universe = grid
  	Should_Transfer_Files = YES
  	When_To_Transfer_Output = ON_EXIT_OR_EVICT
  	Transfer_input_files = /path/to/wu/executable,/path/to/input/file1,/path/to/input/file2
  	log = saga-bj.log

In the "Transfer_input_files" parameter we should specify all the input files that are required to run the subjobs
including the executable. Every allocated Condor-G Bigjob has its own working directory ie the working directory for 
every bigjob. It transfers all the specified input files in to working directory of allocated Condor-G resource. 
So make sure that subjobs job description should reflect these changes.

For example, to run BFAST on a Condor-G resources your "[saga.adaptors.condor_job.default_attributes]" in the file
"saga_adaptor_condor_job.ini" file should look like this
	
	[saga.adaptors.condor_job.default_attributes]
	  Universe = grid
	  Should_Transfer_Files = YES
	  When_To_Transfer_Output = ON_EXIT_OR_EVICT
	  Transfer_input_files = /home/username/software/bfast065/bin/bfast,/home/username/bfast_files/bgr1.fa,/home/username/bfast_files/bgr1.fa.nt.1.1.bif,/home/username/bfast_files/bgr1.fa.nt.brg,/home/username/bfast_files/reads_1.fastq,/home/username/bfast_files/bgr1.fa.nt.2.1.bif,/home/username/bfast_files/bgr1.fa.nt.3.1.bif
	  log = saga-bj.log


and your subjob description should look like this 

    # Submit SubJob through BigJob
    jd = description()

    jd.executable = "bfast"
    jd.number_of_processes = "1"
    jd.spmd_variation = "single"
    jd.arguments = ["match -f  bgr1.fa -A 0  -r reads_1.fastq -n 4 -T /tmp/ > bfast.matches.file.bgr.1.bmf"]
    #jd.working_directory = "" 
    jd.output = "bfast-stdout.txt"
    jd.error = "bfast-stderr.txt"

    sj = subjob()
    sj.submit_job(bj.pilot_url, jd)


4) We used "condorg://brgw1.renci.org:2119/jobmanager-pbs" for testing this example.

** Because of the fact that we cannot install SAGA-C++ on remote Condor-G resources for each Bigjob run, we are 
forced to use stand alone Bigjob Agent ie without SAGA on the resource. So we utilize redis as the co-ordinantion backend 
which work well on backend with Python Version >= 2.5 as the default environment.

5) Please refer to examples/example_condorg_single.py for more details.
