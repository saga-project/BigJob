###############
Chained Example
###############

What if you had two different executables to run? What if this second set of executables had some dependencies on data from A? Can you use one BigJob to run both jobs? Yes!

The below example submits a set of echo jobs (set A) using BigJob, and for every successful job (with state Done), it submits another /bin/echo job (set B) to the same Pilot-Job.

We can think of this as A is comprised of subjobs {a1,a2,a3}, while B is comprised of subjobs {b1,b2,b3}. Rather than wait for each subjob {a1},{a2},{a3} to complete, {b1} can run as soon as {a1} is complete, or {b1} can run as soon as a slot becomes available -- i.e. {a2} could finish before {a1}. 

The code below demonstrates this behavior. As soon as there is a slot available to run a job in B (i.e. a job in A has completed), it executes the job in B. This keeps the BigJob utilization high.

.. literalinclude:: ../../../examples/tutorial/local_chained_ensembles.py
    :language: python