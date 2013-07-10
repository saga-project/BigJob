
#################
Installing BigJob
#################

=================
Environment Setup
=================

This section will explain how to set up your environment and install BigJob.

-----------------
Prerequisites
-----------------
* Python 2.6 or higher. Python 2.7.x is recommended.
* Redis Server
* SAGA-Python Installation (automatically installed when installing BigJob following this guide)

-----------------
Bootstrap your Local Python Environment
-----------------

Assuming you don't want to mess with your system Python installation, you need a place where you can install BigJob locally. A small tool called `virtualenv <http://pypi.python.org/pypi/virtualenv/>`_ allows you to create a local Python software repository that behaves exactly like the global Python repository, with the only difference that you have write access to it. This is referred to as a 'virtual environment.'

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

The last step in this process is to add your newly created virtualenv to your :code:`.bashrc` so that any batch jobs that you submit have the same Python environment as you have on your submitting resource. Add the following line at the end of your :code:`$HOME/.bashrc` file::
	
	source $HOME/.bigjob/python/bin/activate

=================
Install BigJob
=================

After your virtual environment is active, you are ready to install BigJob. BigJob is available via PyPi and can be installed using easy_install as follows::
	
	pip install saga-bigjob

You can change the default installation directory by calling::

	pip install --install-option="--prefix=<target-directory>" saga-bigjob	

To make sure that your installation works, run the following command to check if the BigJob module can be imported by the python interpreter::

	python -c "import pilot; print pilot.version"

=================
Execution Setup
=================

There are two requirements for proper BigJob execution:

#. Agent Directory
#. SSH Password-Less Login

-----------------
Create your Agent Directory
-----------------

BigJob needs a working directory in which to store all of its output, run information, and any errors that may occur. This directory can be named anything you choose, but for any examples in this manual, we will call the directory 'agent' (default). You should create this directory in the same location you run your scripts from, i.e. usually :code:`$SCRATCH` or :code:`$WORK`. You can create this directory by typing::

	mkdir agent

-----------------
SSH Password-Less Login
-----------------

If you are planning to submit from one resource to another, you must have SSH password-less login enabled to the submitting resource. This is achieved by placing your public key on one resource in the authorized_keys file on the target machine. 

Examples of when you would need password-less login: 

#. You want to submit from your local machine to an XSEDE resource
#. You want to submit from one XSEDE resource to another
#. You want to submit from your local cluster to external clusters, etc. etc.

^^^^^^^^^^^^^^^^^
Prerequisites 
^^^^^^^^^^^^^^^^^

* :code:`openssh-server` (if you're running on your own system)
* If running on XSEDE or FutureGrid systems, you do not have prerequisites.

^^^^^^^^^^^^^^^^^
Key Generation and Installation
^^^^^^^^^^^^^^^^^

1. **Generate Public/Private Key Pair**

First, you have to generate a key. You do this as follows:

* Step 1: Use the command :code:`ssh-keygen -t rsa -C <your-e-mail>` to generate the key.
* Step 2: Specify the KeyPair location and name. We recommend that you use the default location if you do not yet have another key there, e.g. :code:`/home/username/.ssh/id_rsa`
* Step 3: Type user defined passphrase when asking passphrase for the key.

Example::

	ssh-keygen -t rsa -C johndoe@email.edu

	Generating public/private rsa key pair. 
	Enter file in which to save the key (/home/johndoe/.ssh/id_rsa):  
	Enter passphrase (empty for no passphrase): 
	Enter same passphrase again: 
	Your identification has been saved in /home/johndoe/.ssh/id_rsa. 
	Your public key has been saved in /home/johndoe/.ssh/id_rsa.pub. 
	The key fingerprint is: 34:87:67:ea:c2:49:ee:c2:81:d2:10:84:b1:3e:05:59 johndoe@email.edu

2. **List the Result**

You can find your key under the key location. As we used the .ssh directory, it will be located there.::

	cd /home/username/.ssh
	ls

Verify that you have created the files :code:`id_rsa` and :code:`id_rsa.pub`.

3. **Capture the Public Key for Target Machine**

Use a text editor to open the :code:`id_rsa.pub` file. Copy the **entire** contents of this file. 

The contents of this file needs to be appended to the target machine's :code:`.ssh/authorized_keys` file. If the authorized_keys file is not accessible, then just create a :code:`.ssh/authorized_keys2` file and paste the key.

Now the target machine is ready to accept your ssh key.

4. **Test your Key Installation**

The ssh-add command tells the machine which keys to use. For a test, type::

	ssh-agent sh -c 'ssh-add < /dev/null && bash'

This will start the ssh-agent, add your default identity (prompting you for your passphrase), and spawn a bash shell.

From this new shell, you should be able to :code:`ssh target_machine`. This should let you in without typing a password or passphrase.

Test whether you have a password-less login to the target machine by executing the simple command::
	
	ssh <hostname> /bin/date

This command should execute without password input.