############
Introduction
############

BigJob is a Pilot-Job framework built on top of The Simple API for Grid Applications (SAGA), a high-level, easy-to-use API for accessing distributed resources. BigJob supports a wide range of application types and is usable over a broad range of infrastructures, i.e., it is general-purpose, extensible, and interoperable. It is written in the python programming language.

=================
Introduction to Pilot-Jobs
=================

Pilot-Jobs support the decoupling of workload submission from resource assignment. This results in a flexible execution model, which in turn enables the distributed scale-out of applications on multiple and possibly heterogeneous resources. Pilot-Jobs support the use of container jobs with sophisticated workflow management to coordinate the launch and interaction of actual computational tasks within the container. It allows the execution of jobs without the necessity to queue each individual job.

=================
Why do you need Pilot-Jobs?
=================

Production-grade distributed cyberinfrastructure almost always has a local resource manager installed, such as a batch queuing system. A distributed application often requires many jobs to produce useful output data; these jobs often have the same executable. A traditional way of submitting these jobs would be to submit an individual job for each executable. These jobs (often hundreds) sit in the batch queuing system and may not become active at the same time. Overall, time-to-completion can take many hours due to load and scheduling variations.

A Pilot-Job provides an alternative approach. It can be thought of as a container job for many sub-jobs. A Pilot-Job acquires the resources necessary to execute the sub-jobs (thus, it asks for all of the resources required to run the sub-jobs, rather than just one sub-job). If a system has a batch queue, the Pilot-Job is submitted to this queue. Once it becomes active, it can run the sub-jobs directly, instead of having to wait for each sub-job to queue. This eliminates the need to submit a different job for every executable, and significantly reduces the time-to-completion.

=================
What makes BigJob different?
=================

Unlike other common Pilot-Job systems, SAGA BigJob: 

#. Natively supports MPI jobs
#. Works on a variety of back-end systems

=================
Supported Adaptors
=================

* **fork** - Allows job execution and file handling on the local machine
* **SSH** - Allows job execution on remote hosts via SSH
* **GSISSH** - Allows job execution on remote hosts via GSISSH
* **PBS(+SSH,+GSISSH)** - Provides local and remote access (SSH+GSISSH) to PBS/Torque clusters
* **SGE(+SSH,+GSISSH)** - Provides local and remote access (SSH+GSISSH) Sun (Oracle) Grid Engine Clusters
* **GRAM** - Uses Globus to submit jobs. Globus certificates are required.
* **Amazon EC2(+SSH)** - Start Virtual Machines and submit jobs to AWS clouds
* **Eucalyptus(+SSH)** - Start Virtual Machines and submit jobs to Eucalyptus clouds
* *Coming Soon:* **SLURM**