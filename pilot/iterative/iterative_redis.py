'''
In-Memory engine based on Redis Cluster
'''
import subprocess
import time
import os, sys
import string
import random
import glob

#import  pilot.iterative.redis-py-cluster.rediscluster
from rediscluster.rediscluster  import RedisCluster

#NODE_LIST=["localhost","localhost","localhost","localhost","localhost","localhost"]
NODE_LIST=["127.0.0.1","127.0.0.1","127.0.0.1","127.0.0.1","127.0.0.1","127.0.0.1"]
REDIS_HOME="/usr/local/redis-3.0.0b2/"
START_PORT=7000



def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

class RedisClusterManager():
    
    def __init__(self, nodes=NODE_LIST):
        self.nodes=nodes
        self.redis_processes=[]
        
    def create_cluster(self):
        port = START_PORT
        nodes = []
        for i in self.nodes:
            print "Start redis instance at: %s:%d"%(i,port) 
            p = subprocess.Popen([os.path.join(REDIS_HOME, "redis-server"), 
                                  "--bind", i, "--port", str(port),  "--cluster-enabled", "yes",
                                  "--cluster-config-file",  "node-"+str(port)+".conf"])
            nodes.append(i+":"+str(port))
            port = port + 1 
            self.redis_processes.append(p)
            
        cmd = [os.path.join(REDIS_HOME, "redis-trib.rb"), "create", "--replicas", "1"]
        for i in nodes:
            cmd.append(i)
        time.sleep(4)
        print "Execute: %s"%str(cmd)
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        p.stdin.write("yes")
            
            
            
    def terminate_cluster(self):
        print "Terminate Cluster"
        for p in self.redis_processes:
            p.terminate()
      
        r = glob.glob("node*.conf")
        for i in r:
            os.remove(i)
            

if __name__ == '__main__':
    #redis_cluster=RedisClusterManager(NODE_LIST)
    #redis_cluster.create_cluster()
    
    # Benchmark    
    startup_nodes = [
        {"host": NODE_LIST[1], "port": START_PORT}
    ]
    
    print "Start Benchmark - connect to: " + str(startup_nodes)
    rc = RedisCluster(startup_nodes, 32, timeout=0.1)
    
    
    for i in range(20,29):
        num_bytes = 2**i
        s = id_generator(num_bytes)
        print ("Set string with len: " + str(len(s)) + " size of: " + str(sys.getsizeof(s)))
        rc.set("hello-" + str(i), s)
    
    #time.sleep(10)
    #redis_cluster.terminate_cluster()