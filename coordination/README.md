Alternative coordination services (EXPERIMENTAL - NOT RECOMMENDED!)
-------------------------------------

BigJob supports different other communication & coordination backends via a plugin mechanism.

### A) SAGA Advert Service

1) Utilise coordination url parameter of BigJob to utilize the Advert Service:

	advert://advert.cct.lsu.edu:8080/ (Postgresql)
	advert://localhost/ (SQLite)


2) Utilise coordination URL parameter of BigJob to connect to Redis instance:

	redis://<hostname>:6379

Standard settings are:

	REDIS_SERVER="localhost"
	REDIS_SERVER_PORT=6379


### B) ZMQ

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

