Chained Ensembles
##################

What if you had two different executables to run? What if this second set of executables had some dependencies on data from A? Can you use one BigJob to run both jobs? Yes!

The below example submits a set of echo jobs (set A) using BigJob, and for every successful job (with state Done), it submits another /bin/echo job (set B) to the same Pilot-Job.

We can think of this as A is comprised of subjobs {a1,a2,a3}, while B is comprised of subjobs {b1,b2,b3}. Rather than wait for each subjob {a1},{a2},{a3} to complete, {b1} can run as soon as {a1} is complete, or {b1} can run as soon as a slot becomes available – i.e. {a2} could finish before {a1}.

The code below demonstrates this behavior. As soon as there is a slot available to run a job in B (i.e. a job in A has completed), it executes the job in B. This keeps the BigJob utilization high. 

=======================
Chained Ensemble Code
=======================

Create a new file ``chained_ensembles.py`` and paste the following code:

.. literalinclude:: ../../../examples/tutorial/chained_ensembles.py
	:language: python

------------------------
How to Edit The Examples
------------------------

You will need to make the necessary changes to ``chained_ensembles.py`` as you did in the previous example. In the case of this tutorial, you can actually copy the "REQUIRED PILOT JOB" section that you filled out from ``simple_ensembles.py`` and paste it over the corresponding section in ``chained_ensembles.py.`` You may need to revisit the previous part of this tutorial to understand how to fill out this section if you skipped it.

The link to the table is reiterated here for your convenience:

`Valid Variable Parameters
<http://saga-project.github.io/BigJob/sphinxdoc/tutorial/table.html>`_.

The important difference between this file and the previous file is that there are two separate "USER DEFINED TASK DESCRIPTION" sections - numbered 1 and 2. Again, these two sections will not require any modifications for the purposes of this tutorial. We will not review every variable again, but instead, review the relationship between the 2 task descriptions.

Go to line 70, "BEGIN USER DEFINED TASK 1 DESCRIPTION." This looks a lot like the description we saw in the previous example. It is also contained in a for loop from 0 to the NUMBER_JOBS. We are running the same executable, with almost the same arguments, except that we append an 'A' as an additional TASK_SET variable. If we look at lines 90-93, we see that as soon as a task in the "A" set reaches the "Done" state, we start what is defined in "BEGIN USER DEFINED TASK 2 DESCRIPTION" as a "B" task. This shows us an important feature of BigJob. We can call get_state() on a task to find out if it is complete or not. The second task description is to run the same executable, /bin/echo, and print instead that it is a B task, with its task number.

To give you an idea of some sample values for NUMBER_OF_PROCESSES (for the Pilot) and NUMBER_JOBS for this Coupled Ensembles tutorial example, reference the machine Lonestar in the table. Note that the valid NUMBER_OF_PROCESSES values are increments of 12. This tells us there are 12 cores per node on Lonestar. A sample job for this example might have NUMBER_JOBS=24 and PILOT_SIZE=24, which would reserve 2 nodes on Lonestar. Note that when running these examples locally, i.e. with SAGA_ADAPTOR set to either ssh or fork, you cannot request a Pilot larger than the number of cores on the machine in which you are running, e.g. a 2-core computer can run a 2-core Pilot, while a 4-core computer can run a 4-core Pilot.

-------------
Run the Code
-------------

Save the file and execute it **(make sure your virtualenv is activated):**

.. code-block:: bash

    python chained_ensembles.py

The output should look something like this:

.. code-block:: none

	* Submitted 'A' task '0' with id 'cu-27ab3846-e9a9-11e2-88eb-14109fd519a1'
	* Submitted 'A' task '1' with id 'cu-27c2cca4-e9a9-11e2-88eb-14109fd519a1'
	[...]
	One 'A' task cu-27ab3846-e9a9-11e2-88eb-14109fd519a1 finished. Launching a 'B' task.
	* Submitted 'B' task '31' with id 'cu-352139c6-e9a9-11e2-88eb-14109fd519a1'
	[...]
	Terminating BigJob...

-----------------
Check the Output
-----------------

Again, we will find all the relevant BigJob output in the directory that we defined as "WORKDIR" in the above example. Note that some of the sj-directories in their stdout.txt files will contain A task output while others will contain B task output.

