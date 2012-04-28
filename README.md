BigJob  
=============

The SAGA BigJob framework is a SAGA-based pilot job implementation. The  Simple API for Grid Applications (SAGA) is a high-level, easy-to-use API for accessing distributed resources. SAGA BigJob supports a wide range of application types, and is usable over a broad range of infrastructures, i.e., it is general-purpose, extensible and interoperable. Unlike other common pilot job systems SAGA BigJob 
(i) natively supports MPI job and 
(ii) works on a variety of back-end systems, generally reflecting the advantage of using a SAGA-based approach. 



Web Page & Mailing List
-----------------------

Web page: https://github.com/saga-project/BigJob/wiki

Mailing list:  
bigjob-user@googlegroups.com: http://groups.google.com/group/bigjob-users
bigjob-devel@googlegroups.com: http://groups.google.com/group/bigjob-devel

Installation
-------------

    $ curl https://raw.github.com/saga-project/BigJob/master/bootstrap/bigjob-bootstrap.py -o bigjob-bootstrap.py
    $ python bigjob-bootstrap.py $HOME/.bigjob/python/
    $ . $HOME/.bigjob/python/bin/activate

See https://github.com/saga-project/BigJob/wiki/ for further information. The Wiki includes among many things information on how to setup BigJob on various machines, e.g. on XSEDE (Ranger, Kraken) or FutureGrid.


Configuration of Coordination Backend
-------------------------------------

BigJob supports different communication & coordination backends via a plugin mechanism.
Currently supported backends:

A) SAGA Advert Service

B) REDIS, http://redis.io/

C) ZMQ (ZeroMQ), http://www.zeromq.org/


### A) SAGA Advert Service

1) Utilise coordination url parameter of BigJob to utilize the Advert Service:

	advert://advert.cct.lsu.edu:8080/ (Postgresql)
	advert://localhost/ (SQLite)


### Redis

1.) Install and run Redis (http://redis.io/)

Mac Homebrew:
	brew install redis redis-tools hiredis

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


2) Utilise coordination URL parameter of BigJob to connect to Redis instance:

	redis://<hostname>:6379

Standard settings are:

	REDIS_SERVER="localhost"
	REDIS_SERVER_PORT=6379


### C) ZMQ

1.) Install ZeroMQ and pyzmq

http://www.zeromq.org/intro:get-the-software
http://zeromq.github.com/pyzmq/index.html

1.1 Install ZMQ:

	$ wget http://download.zeromq.org/zeromq-2.1.7.tar.gz
	$ tar -xzvf zeromq-2.1.7.tar.gz
	$ cd zeromq-2.1.7
	$ ./configure --prefix=$HOME/sw/zeromq-2.1.7
	$ make install


1.2 Activate BigJob virtualenv:

	. $HOME/.bigjob/python/bin/activate

1.3 Install Python bindings:

	$ export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/sw/zeromq-2.1.7/lib/
	$ export ZMQ_DIR=$HOME/sw/zeromq-2.1.9/
	
	$ pip install pyzmq


2) Utilize coordination url parameter of BigJob to utilize ZMQ:

	tcp://*

will start a local tcp server.

---------------------------------------

Packaging
-------------------------------------

Requirements:

*  setuptools >0.6c11, http://pypi.python.org/pypi/setuptools

*  setuptools-git >0.3.3, http://pypi.python.org/pypi/setuptools-git/

Building PyPi package

	python setup.py build

Upload to PyPi

	python setup.py sdist upload
