'''
In-Memory engine based on HDFS In-Memory (Hadoop >2.3)
'''
import subprocess
import time
import os, sys
import string
import random
import copy
import datetime

from webhdfs.webhdfs import WebHDFS 

 
HDFS_URL="hdfs://localhost:50070"
RESULT_FILE_PREFIX="hdfs-inmem"
RESULT_DIR="results"

class HDFSClusterManager():
    
    def __init__(self, nodes=[]):
        self.client =  WebHDFS(self.host, self.port, self.user)
        
    def create_cluster(self):
        pass    
            
            
    def terminate_cluster(self):
        pass


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    s = '%030x' % random.randrange(long(16L**size))
    return s

def get_number_hadoop_nodes():
    fname = "/etc/hadoop/conf/slaves"
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1
    

if __name__ == '__main__':
    
    # Preparation and configuration
    number_of_nodes = get_number_hadoop_nodes()
    number_replicas = 1 
    d = datetime.datetime.now()
    result_filename = RESULT_FILE_PREFIX + d.strftime("%Y%m%d-%H%M%S") + ".csv"
    f = open(os.path.join(RESULT_DIR, result_filename), "w")
    f.write("Size, Time, Backend, NumNodes, NumInstances, Type, NumReplicas\n")



    client =  WebHDFS("ip-10-17-24-243", 
                      50070, 
                      "ec2-user")
    client.rmdir("/tmp/test")
    client.mkdir("/tmp/test/")

    runtimes = {}
    for i in range(20,29):
        num_bytes = 2**i 
        print "PUT File Size: %s MB"%str(num_bytes/1024/1024) 
        s = id_generator(num_bytes)
        print ("Set string with len: " + str(len(s)) + " size of: " + str(sys.getsizeof(s)))
        start=time.time()
        client.put(s, "/tmp/test/test_"+str(num_bytes), number_replicas)
        runtime = time.time()-start                        
        runtimes[num_bytes] = runtime


  
    print "\n*********************************\nResults\n******************************" 
    print "Size, Time, Backend, NumNodes, NumInstances, Type, NumReplicas "
    for key, value in runtimes.iteritems():
        result = str(key) + "," + str(value)+",HDFS,"+str(number_of_nodes)+","+str(number_of_nodes)+",write,"+str(number_replicas)
        print result
        f.write(result + "\n")


    for repeat in range(0,10):
        runtimes={}
        for i in range(20,29):
            num_bytes = 2**i 
            print "GET File Size: %s MB"%str(num_bytes/1024/1024) 
            s = id_generator(num_bytes)
            start=time.time()
            s=client.get("/tmp/test/test_"+str(num_bytes))
            runtime = time.time()-start                        
            runtimes[num_bytes] = runtime

        for key, value in runtimes.iteritems():
            result= str(key) + "," + str(value)+",HDFS,"+str(number_of_nodes)+","+str(number_of_nodes)+",read,"+str(number_replicas)+","+str(repeat)
            print result
            f.write(result + "\n")

    f.close()
