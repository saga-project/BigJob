########################
Mandelbrot using BigJob
########################

In this example, we split up the calculation of a `Mandelbrot set <http://en.wikipedia.org/wiki/Mandelbrot_set>`_ into several `tiles`.

Rather than submit a single job for each tile, we want to submit one BigJob and then execute each individual tile job in a distributed fashion. This is why BigJob is useful. We reserve the resources we need for all of the jobs, but submit just one job that requests all of these resources. Once the job becomes active, the compute units are executed in a distributed fashion. The tiles are then retrieved using features included in BigJob such as the SAGA File API, and the final image is stitched together from the individual tiles. 

.. image:: ../images/mandelbrot.png

-----------------------------------------
Hands-On: Distributed Mandelbrot Fractals
-----------------------------------------

In order for this example to work, we need to install an additional Python module, the `Python Image Library (PIL) <http://www.pythonware.com/products/pil/>`_. This is done via ``pip``::

     pip install PIL

Next, we need to download the `Mandelbrot fractal generator <https://github.com/saga-project/bliss/blob/master/examples/advanced/mandelbrot/mandelbrot.py>`_ itself. It is really just a very simple python script that, if invoked on the command line, outputs a full or part of a Mandelbrot fractal as a PNG image. Download the script into your ``$HOME`` directory::

     curl --insecure -Os https://raw.github.com/saga-project/bliss/master/examples/advanced/mandelbrot/mandelbrot.py

You can give ``mandelbrot.py`` a test-drive locally by calculating a single-tiled 1024x1024 Mandelbrot fractal::

     python mandelbrot.py 1024 1024 0 1024 0 1024 frac.gif

Cut and paste code below into ``bj_mandelbrot.py``.

.. literalinclude:: ../../../examples/tutorial/local_mandelbrot.py
	:language: python


^^^^^^^^^^^^^^^^^^^
Execute the Script
^^^^^^^^^^^^^^^^^^^

Execute your script by typing ``python bj_mandelbrot.py``

^^^^^^^^^^^^^^^^^^^^^^^
Compare to saga-python
^^^^^^^^^^^^^^^^^^^^^^^

Compare the execution of bj_mandelbrot.py to its SAGA counterpart `saga_mandelbrot.py <http://saga-project.github.io/saga-python/doc/tutorial/part5.html>`_. Notice that a Pilot-Job is used in the BigJob case to submit many jobs at one time, instead of submitting them serially as in the SAGA example.
