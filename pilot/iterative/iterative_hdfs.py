'''
In-Memory engine based on Tachyon
'''
import subprocess
import time
import os, sys
import string
import random
import copy

from webhdfs.webhdfs import WebHDFS 

 
HDFS_URL="hdfs://localhost:50070"


class HDFSClusterManager():
    
    def __init__(self, nodes=NODE_LIST):
        self.client =  WebHDFS(self.host, self.port, self.user)
        
    def create_cluster(self):
        pass    
            
            
    def terminate_cluster(self):
        pass


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    s = '%030x' % random.randrange(16**size)
    return s

if __name__ == '__main__':
    
    
    client =  WebHDFS("localhost", 
                      50070, 
                      "ec2-user")
    client.mkdir("/tmp/test/")
    runtimes = {}
    for i in range(20,32):
        num_bytes = 2**i 
        s = id_generator(num_bytes)
        start=time.time()
        print ("Set string with len: " + str(len(s)) + " size of: " + str(sys.getsizeof(s)))
        client.put(s, "/tmp/test")        
        runtime = time.time()-start                        
        runtimes[num_bytes] = runtime
        
    print "\n*********************************\nResults\n******************************" 
    print "Size,Time,Backend"
    for key, value in runtimes.iteritems():
        print str(key) + "," + str(value)+",Tachyon"
            
            
                       
    
