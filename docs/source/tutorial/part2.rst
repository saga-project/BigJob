##################
Simple Ensembles
##################

You might be wondering how to create your own BigJob script or how BigJob can be useful for your needs. Before delving into the remote job and data submission capabilities that BigJob has, its important to understand the basics. 

========================
Hands-On Job Submission
========================

The simplest usage of a pilot-job system is to submit multiple identical tasks collectively, i.e. as one big job! Such usage arises, for example to perform either a parameter sweep job or a set of ensemble simulation.

We will create an example which submits N jobs using BigJob. The jobs are all identical, except that they each record their number in their output. This type of run is called a "simple ensemble" run; it is very useful if you are running many jobs using the same executable (but perhaps different input files). Rather than submit each job individually to the queuing system and then wait for every job to become active and complete, you submit just one 'Big' job (called a Pilot) that reserves the number of cores needed to run all of your jobs. When this BigJob becomes active, your jobs are pulled by BigJob from the Redis server and executed. 

Create a new file ``simple_ensembles.py`` and paste the following code:

.. literalinclude:: ../../../examples/tutorial/simple_ensembles.py
	:language: python

------------------------
How to Edit The Examples
------------------------

Open the file ``simple_ensembles.py.`` There are two critical sections that must be filled in by the user. We will cover both in this tutorial. 

Line 11 of this file says, "BEGIN REQUIRED PILOT SETUP." All of the variables in this section are required in order for your BigJob script to run. You might be wondering what values are valid for these variables; we have created a table to answer these questions.
`Click here to visit the table of valid parameters
<http://saga-project.github.io/BigJob/sphinxdoc/tutorial/table.html>`_.

We will go over the variables not listed in the table below.

REDIS_PWD and REDIS_URL correspond to the redis server that you set up during the installation process of this tutorial. If you are not running the redis on localhost, you must change the URL line. Please do not delete the ``redis://`` directive, as this tells BigJob that it is using a redis server.

USER_NAME: This does not appear in the table, because it corresponds to your individual username on the remote resource. For instance, if the machine you want to run BigJob on has a user account corresponding to jdoe, the USER_NAME field will be ``jdoe.``

WORKDIR: This field is required. It tells BigJob where to place your files. If you recall, in the installation guide for this tutorial, we created an "agent" directory in our home directory. If our home directory on the resource where we created the agent directory is ``/N/u/username``, then our BigJob working directory (WORKDIR) would be ``/N/u/username/agent``.

Special Note: When you see "N/A" in the table, you should fill in None in the simple_ensembles.py file. For example, if the machine you are running on does not have a PROJECT allocation, this line will look like:

``PROJECT      = None # Add project / allocation / account to charge``

NUMBER_JOBS: This is the number of tasks you want to run. Note that this may be different than the PILOT_SIZE. You may want to run 32 executables of 2 cores each, i.e. NUMBER_JOBS=32, but PILOT_SIZE=64 (if all are to run at once). For the purposes of this tutorial, a good value for the NUMBER_JOBS = 4.

Line 44, "END REQUIRED PILOT SETUP," concludes this section.

Now, let's take a look at how these variables are used to make the "Pilot Compute Description (PCD)" on Lines 51-58. You do not have to modify the PCD at all, because it uses the variables we filled in during the REQUIRED PILOT SETUP section. This is just to show you how these variables are communicated to BigJob. The PCD describes all the parameters for the Pilot-Job.

.. code-block:: python

        pilot_description = pilot.PilotComputeDescription()
        pilot_description.service_url = "%s://%s@%s" %  (SAGA_ADAPTOR,USER_NAME,HOSTNAME)
        pilot_description.queue = QUEUE
        pilot_description.project = PROJECT
        pilot_description.number_of_processes = PILOT_SIZE
        pilot_description.working_directory = WORKDIR
        pilot_description.walltime = WALLTIME
        pilot_description.processes_per_node = PROCESSES_PER_NODE
        pilot_description.spmd_variation = SPMD_VARIATION

The second important section of this code will not require any modification for the tutorial, but we cover it here so you can see how to modify it for your own purposes.

Go to Line 68, "BEGIN USER DEFINED TASK DESCRIPTION." This is where you will define the jobs that the overall Pilot-Job will be executing. This normally includes the path to your specific executable and any inputs or environment setup that is required. It is copied below for discussion purposes:

.. code-block:: python

        # -------- BEGIN USER DEFINED TASK DESCRIPTION --------- #
            task_desc = pilot.ComputeUnitDescription()
            task_desc.executable = '/bin/echo'
            task_desc.arguments = ['I am task number $TASK_NO', ]
            task_desc.environment = {'TASK_NO': i}
	    task_desc.spmd_variation = single # Valid values are single or mpi
            task_desc.number_of_processes = 1
            task_desc.output = 'simple-ensemble-stdout.txt'
            task_desc.error = 'simple-ensemble-stderr.txt'
        # -------- END USER DEFINED TASK DESCRIPTION --------- #

Let's discuss the above example. We define our executable as "/bin/echo," the simple UNIX command that writes arguments to standard output. Next, we need to provide the arguments. In this case, "I am task number $TASK_NO," would correspond to typing ``/bin/echo 'I am task number $TASK_NO'`` on command line. ``$TASK_NO`` is an environment variable, so we will need to provide a value for it, as is done on the next line: ``{'TASK_NO': i}``. Note that this block of code is in a python for loop, therefore, i corresponds to what iteration we are on. The spmd_variation for a job can be either single or mpi. This refers to the way BigJob attempts to run your job (and also is dependent on the application you are running) - use MPI if your executable uses MPI, otherwise just use single. We then define number_of_processes for the task - this is the number of cores that the task requires (think, for example, of a 4-core MPI executable - that would mean 1 executable requires 4-cores, i.e. the number_of_processes = 4). Note that a spmd_variation of 'single' can also require multiple cores. 

Lastly, we define the filenames for the tasks as output and error - these capture the stdout and stderr of running the task itself. These files will not capture BigJob behavior or error messages - those will be discussed later. The directory structure of BigJob will be discussed after we run this example.

-------------
Run the Code
-------------

Save the file and execute it **(make sure your virtualenv is activated):**

.. code-block:: bash

    python simple_ensembles.py

The output should look something like this:

.. code-block:: none

	* Submitted task 1 with id 8044 to localhost
	Waiting for tasks to finish...
	Terminating BigJob...

-----------------
Check the Output
-----------------

**Steps to interpreting your output:**

1.) We will find all the relevant BigJob output in the directory that we defined as "WORKDIR" in the above example.

``cd`` into your "working directory." In the examples on this tutorial, this would be the ``$HOME/agent`` directory.

2.) The agent directory contains a directory that is named after the Pilot Service. This directory begins with ``bj-####``, where ### is a unique identifier. 

Example:

.. code-block:: bash

	(python)-bash-3.2$ cd agent
	(python)-bash-3.2$ ls
	bj-bac51654-d96b-11e2-8773-a4badb0c3696			  
	stdout-bj-bac51654-d96b-11e2-8773-a4badb0c3696-agent.txt
	stderr-bj-bac51654-d96b-11e2-8773-a4badb0c3696-agent.txt

``cd`` into the ``bj-####`` directory.


3.) If you do an ``ls`` in this directory, you should see a number of directories that begin with ``sj-###`` where ### is a unique identifier. These sj-directories represent each task that ran. For example, if NUMBER_JOBS=4, then we ran 4 tasks in the above example, so we would expect 4 sub-job directories as follows:

.. code-block:: bash

	(python)-bash-3.2$ cd bj-bac51654-d96b-11e2-8773-a4badb0c3696/
	(python)-bash-3.2$ ls
	sj-bb1dcfb0-d96b-11e2-8773-a4badb0c3696  
	sj-bb634946-d96b-11e2-8773-a4badb0c3696
	sj-bb409194-d96b-11e2-8773-a4badb0c3696  
	sj-bb85fd9c-d96b-11e2-8773-a4badb0c3696

4.) Contained within each sub-job directory is a stdout.txt (which contains the output of your executable) and stderr.txt (which contains the output of standard error on the command line -- it will be blank if there were no errors when running your executable) file. Please note that if you defined different names for the "output" and "error" fields in your compute unit description, then the names stdout and stderr will vary.

Example:

.. code-block:: bash

	(python)-bash-3.2$ cd sj-bb1dcfb0-d96b-11e2-8773-a4badb0c3696/
	(python)-bash-3.2$ ls
        simple-ensemble-stdout.txt
        simple-ensemble-stderr.txt

5.) Open simple-ensemble-stdout.txt and note the output from your script.

----------------------
Logging and Debugging
----------------------

Since working with distributed systems is inherently complex and much of the
complexity is hidden within BigJob, it is necessary to do a lot of internal
logging. By default, logging output is disabled, but if something goes wrong or
if you're just curious, you can enable the logging output by setting the
environment variable ``BIGJOB_VERBOSE`` to a value between 1 (print only critical
messages) and 5 (print all messages). Give it a try with the above example:

.. code-block:: bash

  BIGJOB_VERBOSE=5 python simple_ensembles.py
