###############
Simple Ensemble
###############

You might be wondering how to create your own BigJob script or how BigJob can be useful for your needs. 

The first example, below, submits N jobs using BigJob. This is very useful if you are running many jobs using the same executable. Rather than submit each job individually to the queuing system and then wait for every job to become active and complete, you submit just one 'Big' job that reserves the number of cores needed to run all of your jobs. When this BigJob becomes active, your jobs are pulled by BigJob from the Redis server and executed. 

The below examples demonstrates the mapping of a simple job (i.e. executable is /bin/echo) using all of the parameters of a Compute Unit Description. Specifically, it shows how to run 4 jobs on your local machine using fork:

.. literalinclude:: ../../../examples/tutorial/local_simple_ensembles.py
    :language: python