##################
Run BigJob Locally
##################

You might be wondering how to create your own BigJob script or how BigJob can be useful for your needs. Before delving into the remote job and data submission capabilities that BigJob has, its important to understand the basics. The easiest way to understand the most basic functionality BigJob has to offer is to run multiple jobs on your local laptop. 

=============================
Hands-On Local Job Submission
=============================

We will create an example which submits 4 jobs using BigJob. The jobs are all identical, except that they each record their number in their output. This type of run is called a "simple ensemble" run; it is very useful if you are running many jobs using the same executable (but perhaps different input files). Rather than submit each job individually to the queuing system and then wait for every job to become active and complete, you submit just one 'Big' job (called a Pilot) that reserves the number of cores needed to run all of your jobs. When this BigJob becomes active, your jobs are pulled by BigJob from the Redis server and executed. 

Create a new file ``bigjob_example_local.py`` and paste the following code:

.. literalinclude:: ../../../examples/tutorial/local_simple_ensembles.py
	:language: python

-------------
Run the Code
-------------

Save the file and execute it **(make sure your virtualenv is activated):**

.. code-block:: bash

    python bigjob_example_local.py

The output should look something like this:

.. code-block:: none

	Finished Pilot-Job setup. Submitting compute units
	Waiting for compute units to complete
	Terminate Pilot Jobs

-----------------
Check the Output
-----------------

**Steps to interpreting your output:**

1.) Remember in the Installation process when we created an agent directory where BigJob worked its magic? The agent directory is where we will find our output!

``cd`` into your "working directory." In the examples on this tutorial, this would be the ``$HOME/agent`` directory.

2.) The agent directory contains a directory that is named after the Pilot Service. This directory begins with ``bj-####``, where ### is a unique identifier. ``cd`` into the ``bj-####`` directory.

Example:

.. code-block:: bash

	(python)-bash-3.2$ cd agent
	(python)-bash-3.2$ ls
	bj-bac51654-d96b-11e2-8773-a4badb0c3696			  
	stdout-bj-bac51654-d96b-11e2-8773-a4badb0c3696-agent.txt
	stderr-bj-bac51654-d96b-11e2-8773-a4badb0c3696-agent.txt


3.) If you do an ``ls`` in this directory, you should see a number of directories that begin with ``sj-###`` where ### is a unique identifier. These sj-directories represent each job that ran. For example, we ran 4 jobs in the above example (as evidenced by NUMBER_JOBS=4), so we can expect 4 sub-job directories as follows:

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
	stderr.txt  
	stdout.txt

5.) Open stdout.txt and note the output from your script.

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

  BIGJOB_VERBOSE=5 python bigjob_example_local.py