####################################
Simple Ensembles with File Transfer
####################################

So far, we have covered three different examples which submit jobs to remote resources. In these examples, we had to log on to the remote resource in order to see our data (in the agent directory). This example will cover how we get the files back on our local submission machine. 

========================
Data Transfer Code
========================

Create a new file ``simple_ensemble_dataxfer.py`` and paste the following code:

.. literalinclude:: ../../../examples/tutorial/simple_ensemble_dataxfer.py
	:language: python

------------------------
How to Edit The Examples
------------------------

Open the file ``simple_ensemble_dataxfer.py.`` You will need to make the necessary changes to the BEGIN REQUIRED PILOT SETUP as you have been making to the previous examples. It is sufficient to copy-paste from the previous examples if you are submitting to the same remote resource.

You may need to revisit the previous part of this tutorial to understand how to fill out this section if you skipped it.

The link to the table is reiterated here for your convenience:

`Valid Variable Parameters
<http://saga-project.github.io/BigJob/sphinxdoc/tutorial/table.html>`_.

The logic of this file is exactly the same as in the first tutorial example, Simple Ensembles. The only addition to this file is beginning on Line 91, "BEGIN FILE TRANSFER LOGIC." It is pasted below:

.. code-block:: python

        # ------------ BEGIN FILE TRANSFER LOGIC ------------- #
        # all compute units have finished. now we can use saga-python
        # to transfer back the output files...
        d = saga.filesystem.Directory("sftp://%s/%s" % (HOSTNAME,WORKDIR))
        for task in tasks:
            local_filename = "ex-2-stdout-%s.txt" % (task.get_id())
            d.copy("%s/stdout.txt" % (task.get_local_working_directory()), "file://localhost/%s/%s" % (os.getcwd(), local_filename))
            print "* Output for '%s' copied to: './%s'" % (task.get_id(), local_filename)
        # ------------ END FILE TRANSFER LOGIC  ------------- #

You do not have to change this logic. It uses the saga-python file transfer capabilities to get the output back to the submitting resource. Let's review how it does this. First, a handle to the directory where the files are located are obtained.

-------------
Run the Code
-------------

Save the file and execute it **(make sure your virtualenv is activated):**

.. code-block:: bash

    python simple_ensemble_dataxfer.py

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
