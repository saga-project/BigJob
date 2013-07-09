
#########################
Setting Up a Redis Server
#########################

BigJob uses a Redis server for coordination and task management. Redis is the most stable and fastest backend (requires Python >2.5) and the recommended way of using BigJob. BigJob will **not** work without a coordination backend.

Redis can easily be run in user space. For additional information about redis, please visit the website, `redis.io<http://www.redis.io>`_. To install your own redis server, please take the following steps::

	wget http://download.redis.io/redis-stable.tar.gz
	tar xvzf redis-stable.tar.gz
	cd redis-stable
	make

Once you have downloaded and installed it, start a Redis server on the machine of your choice as follows::

	$ cd redis-stable
	$ ./src/redis-server 
	[489] 13 Sep 10:11:28 # Warning: no config file specified, using the default config. In order to specify a config file use 'redis-server /path/to/redis.conf'
	[489] 13 Sep 10:11:28 * Server started, Redis version 2.2.12
	[489] 13 Sep 10:11:28 * The server is now ready to accept connections on port 6379
	[489] 13 Sep 10:11:28 - 0 clients connected (0 slaves), 922160 bytes in use

You can install redis on a persistent server and use this server as your dedicated coordination server.




