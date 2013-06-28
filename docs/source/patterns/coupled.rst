#################
Coupled Ensembles
#################

The script provides a simple workflow which submit a set of jobs(A) and jobs(B) and wait until they are completed and then submits set of jobs(C). It demonstrates synchronization mechanisms provided by the Pilot-API. This example is useful if an executable C has dependencies on some of the output generated from jobs A and B.

.. literalinclude:: ../../../examples/tutorial/local_coupled_ensembles.py
    :language: python