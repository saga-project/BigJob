# YARN BigJob Plugin

## Requirements:

* local Hadoop 2.2.0 installation

## BigJob YARN Application (binary within plugin directory)
This plugin depends on the BigJob YARN Application Master found here:

<https://github.com/drelu/BigJob-YARN>

A build of the BigJob YARN application is distributed with BigJob.

	BIGJOB_SOURCE_DIRECTORY/bigjob/job_plugin/yarn/BigJob-YARN-0.1-SNAPSHOT-jar-with-dependencies.jar

## Configuration

The YARN and Mesos adaptors currently require some manually configurations of some command utility paths:

	BIGJOB_SOURCE_DIR`/bigjob/job_plugin/yarn 
	BIGJOB_SOURCE_DIR`/bigjob/job_plugin/mesos 

Before using BigJob with YARN please go through the settings within the adaptor. 
For YARN e.g. the file `BIGJOB_SOURCE_DIR`/bigjob/job_plugin/yarn/yarn.py` needs 
to be modified to reflect the location of your Hadoop/YARN installation:

    """ HADOOP/JAVA Configuration"""
	JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk1.7.0_40.jdk/Contents/Home"
	YARN_HOME="/usr/local/hadoop-2.2.0/"

`JAVA_HOME` and `YARN_HOME` need to be adjusted to your environment.

Reinstall BigJob from BigJob source directory:

    python setup.py install
    
    
## Example Usage

Pilot Description:

	 pilot_compute_description = {
                                    "service_url": 'yarn://localhost:8032?fs=hdfs://localhost:9000',
                                    "number_of_processes": 1                            
                                  }

The URL consists of the YARN resource manager address and the HDFS namenode address (required for file staging). If the cluster
has a shared filesystem no `fs=` parameter needs to be provided.

see <https://github.com/saga-project/BigJob/blob/develop/examples/hadoop/example-pilot-compute-yarn.py>
    
## Where are my output files?

YARN creates a filesystem sandbox for each container on the local filesystem. The location can be
modified in the `yarn-site.xml`; by default these can be found at:

	/tmp/hadoop-<username/nm-local-dir/usercache/<username>/appcache
	
For each application a directory is created:

	appcache$ ls -lt
	total 0
	drwx--x---  5 170 Jan  1 18:07 application_1388062134656_0006
	drwx--x---  5 170 Jan  1 18:03 application_1388062134656_0005
	drwx--x---  5 170 Jan  1 17:53 application_1388062134656_0004
	drwx--x---  5 170 Jan  1 17:35 application_1388062134656_0002


Each BigJob YARN application consists of 2 containers:

	application_1388062134656_0006$ ls -lt
	total 0
	drwx--x---  11 374 Jan  1 18:07 container_1388062134656_0006_01_000002
	drwxr-xr-x   5 170 Jan  1 18:07 filecache
	drwx--x---  11 374 Jan  1 18:07 container_1388062134656_0006_01_000001

`container_xxxxxxxxxxxxx_xxxx_xx_000001 is the Application Master, `container_xxxxxxxxxxxxx_xxxx_xx_000002` the BigJob agent, i.e.
the default working directory is set to `container_xxxxxxxxxxxxx_xxxx_xx_000002`.

This directory contains the BigJob Agent sandbox with the output of the subjobs/compute units:

	container_1388062134656_0006_01_000002 luckow$ ls -lt
	total 32
	drwxr-xr-x  3 luckow  wheel   102 Jan  1 18:07 bj-8652456e-7339-11e3-9ce8-28cfe9181339

Also, a custom working directory for the BigJob agent can be specified in the pilot description. 