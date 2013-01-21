
#################
Installing BigJob
#################

=================
Environment Setup
=================

-----------------
Prerequisites
-----------------
* Python 2.6 or higher. Python 2.7.x is recommended.
* Redis Server
* SAGA-Python Installation (automatically installed when installing BigJob following this guide)

-----------------
Bootstrap your Local Python Environment
-----------------

Assuming you don't want to mess with your system Python installation, you need a place were you can install BigJob locally. A small tool called `virtualenv <http://pypi.python.org/pypi/virtualenv/>`_ allows you to create a local Python software repository that behaves exactly like the global Python repository, with the only difference that you have write access to it. This is referred to as a 'virtual environment.'

To create your local Python environment run the following command (you can install virtualenv on most systems via apt-get or yum, etc.)::

	virtualenv $HOME/.bigjob/python

If you don't have virtualenv installed and you don't have root access to your machine, you can use the following script instead::

	curl --insecure -s https://raw.github.com/pypa/virtualenv/master/virtualenv.py | python - $HOME/.bigjob/python

-----------------
Activate your Local Python Environment
-----------------

You need to *activate* your Python environment in order to make it work. Run the command below. It will temporarily modify your :code:`PYTHONPATH` so that it points to :code:`$HOME/.bigjob/python/lib/python2.7/site-packages/` instead of the the system site-package directory::

	source $HOME/.bigjob/python/bin/activate

Activating the virtualenv is *very* important. If you don't activate your virtual Python environment, the rest of this tutorial **will not work.** You can usually tell that your environment is activated properly if your bash command-line prompt starts with :code:`(python)`.

