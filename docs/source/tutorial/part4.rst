Coupled Ensembles
##################

The script provides a simple workflow which submit a set of jobs(A) and jobs(B) and wait until they are completed and then submits set of jobs(C). It demonstrates synchronization mechanisms provided by the Pilot-API. This example is useful if an executable C has dependencies on some of the output generated from jobs A and B.

=======================
Coupled Ensemble Code
=======================

Create a new file ``coupled_ensembles.py`` and paste the following code:

.. literalinclude:: ../../../examples/tutorial/coupled_ensembles.py
	:language: python

------------------------
How to Edit The Examples
------------------------

You will need to make the necessary changes to ``coupled_ensembles.py`` as you did in the previous example. In the case of this tutorial, you can actually copy the "REQUIRED PILOT JOB" section that you filled out from ``simple_ensembles.py`` and paste it over the corresponding section in ``coupled_ensembles.py.`` You may need to revisit the previous part of this tutorial to understand how to fill out this section if you skipped it.

The link to the table is reiterated here for your convenience: Table of `Valid Variable Parameters<http://saga-project.github.io/BigJob/sphinxdoc/tutorial/table.html>`_.

The important difference between this file and the previous file is that there are two separate "USER DEFINED TASK DESCRIPTION" sections - numbered 1-3. Again, these two sections will not require any modifications for the purposes of this tutorial. We will not review every variable again, but instead, review the relationship between the 3 task descriptions. The three task descriptions are identical except that they each have a different TASK_SET variable assigned - either A, B, or C. 

NOTE that we call each task set the same number of times (i.e. NUMBER_JOBS) in the tutorial code, but this is not a requirement. It just simplifies the code for tutorial purposes. It is possible you want to run 16 A, 16 B, and then 32 C using the output from both A and B. An example from the table would be to use the resource Stampede which has 16 cores per node (see NUMBER_OF_PROCESSES column of Table) - we can therefore reserve NUMBER_JOBS=32 with a PILOT_SIZE=32 (because 32 is a multiple of 16). In this case, we are requesting 2 nodes from Stampede.

In this case, the important logic to draw your attention too is on line 106-108:

.. code-block:: python

        print "Waiting for 'A' and 'B' tasks to complete..."
        pilotjob.wait()
        print "Executing 'C' tasks now..."

Lines 106 and 108 are simply print statements, but they provide valuable context. In this example, we submit both the A and B tasks to the Pilot, but instead of running C tasks right away, we call ``wait()`` on the pilotjob itself. This tells BigJob to wait for all of the submitted tasks to finish, before continuing in the code. After all the A and B (submitted tasks) have finished, it then submits the C tasks. 

-------------
Run the Code
-------------

Save the file and execute it **(make sure your virtualenv is activated):**

.. code-block:: bash

    python coupled_ensembles.py

The output should look something like this (based on NUMBER_JOBS=32, PILOT_SIZE=32):

.. code-block:: none

	* Submitted 'A' task '0' with id 'cu-833b3762-e9ac-11e2-b250-14109fd519a1'
	* Submitted 'A' task '1' with id 'cu-8352c0f8-e9ac-11e2-b250-14109fd519a1'
	[...]
	* Submitted 'A' task '31' with id 'cu-86137aee-e9ac-11e2-b250-14109fd519a1'
	* Submitted 'B' task '0' with id 'cu-862ad342-e9ac-11e2-b250-14109fd519a1'
	[...]
	* Submitted 'B' task '31' with id 'cu-88fe4c2a-e9ac-11e2-b250-14109fd519a1'
	Waiting for 'A' and 'B' tasks to complete...
	* Submitted 'C' task '0' with id 'cu-ffb024ce-e9ac-11e2-b250-14109fd519a1'
	[...]
	* Submitted 'C' task '31' with id 'cu-0281b708-e9ad-11e2-b250-14109fd519a1'
	Waiting for 'C' tasks to complete...
	Terminating BigJob...

-----------------
Check the Output
-----------------

Again, we will find all the relevant BigJob output in the directory that we defined as "WORKDIR" in the above example. Note that some of the sj-directories in their stdout.txt files will contain either A, B, or C output. 

