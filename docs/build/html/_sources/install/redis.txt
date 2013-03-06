
#########################
Setting Up a Redis Server
#########################

BigJob uses a Redis server for coordination and task management. Redis is the most stable and fastest backend (requires Python >2.5) and the recommended way of using BigJob. 

Redis can easily be run in user space. It can be downloaded at: http://redis.io/download (just ~500 KB). Once you have downloaded it, start a Redis server on the machine of your choice as follows::

	$ redis-server 
	[489] 13 Sep 10:11:28 # Warning: no config file specified, using the default config. In order to specify a config file use 'redis-server /path/to/redis.conf'
	[489] 13 Sep 10:11:28 * Server started, Redis version 2.2.12
	[489] 13 Sep 10:11:28 * The server is now ready to accept connections on port 6379
	[489] 13 Sep 10:11:28 - 0 clients connected (0 slaves), 922160 bytes in use

You can install redis on a persistent server and use this server as your dedicated coordination server.

**Coming Soon:** XSEDE-Specific Redis server. 


