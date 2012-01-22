BigJob  
=============

The SAGA BigJob framework is a SAGA-based pilot job implementation. The  Simple API for Grid Applications (SAGA) is a high-level, easy-to-use API for accessing distributed resources. SAGA BigJob supports a wide range of application types, and is usable over a broad range of infrastructures, i.e., it is general-purpose, extensible and interoperable. Unlike other common pilot job systems SAGA BigJob 
(i) natively supports MPI job and 
(ii) works on a variety of back-end systems, generally reflecting the advantage of using a SAGA-based approach. 



Web Page & Mailing List
-------------

Web page: http://faust.cct.lsu.edu/trac/bigjob

Mailing list:  Use https://mail.cct.lsu.edu/mailman/listinfo/bigjob-users to subscribe to bigjob-users mailing list. The mailing list archives are located at:  https://mail.cct.lsu.edu/pipermail/bigjob-users/.

Installation
-------------

$ curl https://svn.cct.lsu.edu/repos/saga-projects/applications/bigjob/trunk/generic/bootstrap/bigjob-bootstrap.py -o bigjob-bootstrap.py
$ python bigjob-bootstrap.py $HOME/.bigjob/python/
$ . $HOME/.bigjob/python/bin/activate

See http://faust.cct.lsu.edu/trac/bigjob for further information. The Wiki includes among many things information on how to setup BigJob on various machines, e.g. on XSEDE (Ranger, Kraken) or FutureGrid.


Configuration of Coordination Backend
-------------------------------------

BigJob supports different communication & coordination backends via a plugin mechanism.
Currently supported backends:

A) SAGA Advert Service

B) REDIS, http://redis.io/

C) ZMQ (ZeroMQ), http://www.zeromq.org/


### A) SAGA Advert Service

1) Utilise coordination url parameter of BigJob to utilize the Advert Service:

	advert://advert.cct.lsu.edu:8080/ (Postgresql)
	advert://localhost/ (SQLite)


### Redis

1.) Install and run Redis (http://redis.io/)

Mac Homebrew:
	brew install redis redis-tools hiredis

Linux:
Download Redis http://redis.io/download
Currently only tested with Redis 2.2.12

	$ tar -xzvf redis-2.2.12.tar.gz
	$ cd redis-2.2.12
	$ make
	$ make install (if root)


Start Redis (redis-server executable is located in src/ dir):

	cd src
	./redis-server


2) Utilise coordination URL parameter of BigJob to connect to Redis instance:

	redis://cyder.cct.lsu.edu:6379

Standard settings are:

	REDIS_SERVER="localhost"
	REDIS_SERVER_PORT=6379


### C) ZMQ

1.) Install ZeroMQ and pyzmq

http://www.zeromq.org/intro:get-the-software
http://zeromq.github.com/pyzmq/index.html

1.1 Install ZMQ:

	$ wget http://download.zeromq.org/zeromq-2.1.7.tar.gz
	$ tar -xzvf zeromq-2.1.7.tar.gz
	$ cd zeromq-2.1.7
	$ ./configure --prefix=$HOME/sw/zeromq-2.1.7
	$ make install


1.2 Activate BigJob virtualenv:

	. $HOME/.bigjob/python/bin/activate

1.3 Install Python bindings:

	$ export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/sw/zeromq-2.1.7/lib/
	$ export ZMQ_DIR=$HOME/sw/zeromq-2.1.9/
	
	$ pip install pyzmq


2) Utilize coordination url parameter of BigJob to utilize ZMQ:

	tcp://*

will start a local tcp server.

---------------------------------------

Packaging
-------------------------------------

Requirements:

*  setuptools >0.6c11, http://pypi.python.org/pypi/setuptools

*  setuptools-git >0.3.3, http://pypi.python.org/pypi/setuptools-git/

Building PyPi package

	python setup.py build

Upload to PyPi

	python setup.py sdist upload


---------------------------------------

Bigjob with OSG-Condorg
-------------------------------------

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
