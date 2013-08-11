BigJob  
=============

The SAGA BigJob framework is a SAGA-based pilot job implementation. The  Simple API for Grid Applications (SAGA) is a high-level, easy-to-use API for accessing distributed resources. SAGA BigJob supports a wide range of application types, and is usable over a broad range of infrastructures, i.e., it is general-purpose, extensible and interoperable. Unlike other common pilot job systems SAGA BigJob 
(i) natively supports MPI job and 
(ii) works on a variety of back-end systems, generally reflecting the advantage of using a SAGA-based approach. 



Web Page & Mailing List
-----------------------

Web page: <http://saga-project.github.io/BigJob/>

Mailing list:  

[bigjob-user@googlegroups.com](http://groups.google.com/group/bigjob-users)

[bigjob-devel@googlegroups.com](http://groups.google.com/group/bigjob-devel)


Installation
-------------

    $ curl https://raw.github.com/saga-project/BigJob/master/bootstrap/bigjob-bootstrap.py -o bigjob-bootstrap.py
    $ python bigjob-bootstrap.py $HOME/.bigjob/python/
    $ . $HOME/.bigjob/python/bin/activate

See https://github.com/saga-project/BigJob/wiki/ for further information. The Wiki includes among many things information on how to setup BigJob on various machines, e.g. on XSEDE (Ranger, Kraken) or FutureGrid.


Configuration of Redis Coordination Backend
-------------------------------------

BigJob requires [Redis](http://redis.io) for communication between BigJob manager/agent. Use the following steps to install Redis. 

Mac Homebrew:
	
	brew install redis redis-tools 

Linux:
Download Redis http://redis.io/download
Currently only tested with Redis 2.2.12

	$ tar -xzvf redis-2.2.12.tar.gz
	$ cd redis-2.2.12
	$ make
	$ make install (if root)


Start Redis (redis-server executable is located in src/ dir):

	cd src
	./redis-server

Examples: 
-------------------------------------

<https://github.com/saga-project/BigJob/tree/master/examples/pilot-api>

Please modify COORDINATION_URL on top of example to the correct Redis backend!

Development & Packaging
-------------------------------------

Requirements:

*  setuptools >0.6c11, http://pypi.python.org/pypi/setuptools

*  setuptools-git >0.3.3, http://pypi.python.org/pypi/setuptools-git/

Building PyPi package

	python setup.py build

Upload to PyPi

	python setup.py sdist upload

