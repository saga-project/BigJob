'''
In-Memory engine based on Tachyon
'''
import subprocess
import time
import os, sys
import string
import random
import copy

#NODE_LIST=["localhost","localhost","localhost","localhost","localhost","localhost"]
NODE_LIST=["127.0.0.1","127.0.0.1","127.0.0.1","127.0.0.1","127.0.0.1","127.0.0.1"]


#JAVA="/Library/Java/JavaVirtualMachines/jdk1.7.0_40.jdk/Contents/Home/bin/java"
JAVA="java"
TACHYON_HOME="/usr/local/tachyon-0.4.1/"
TACHYON_JAR=os.path.join(TACHYON_HOME, "target/tachyon-0.4.1-jar-with-dependencies.jar") 
TACHYON_URL="tachyon://localhost:19998"


class TachyonClusterManager():
    
    def __init__(self, nodes=NODE_LIST):
        self.nodes=nodes
        self.redis_processes=[]
        
    def create_cluster(self):
        pass    
            
            
    def terminate_cluster(self):
        pass


if __name__ == '__main__':
    tc = TachyonClusterManager()
    tc.create_cluster()
    
    base_args = [JAVA, "-cp" , TACHYON_JAR, "tachyon.examples.Performance"]
    
    os.system("tachyon tfs rm /")
    
    runtimes = {}
    for i in range(20,29):
        num_bytes = 2**i 
        args = copy.deepcopy(base_args)
        args.extend([TACHYON_URL, "/test-" + str(num_bytes), str(num_bytes), "1", "false",  "1", "1", "1", "1"])
        print str(args)
        start=time.time()
        p = subprocess.Popen(args)
        p.wait()         
        runtime = time.time()-start                        
        runtimes[num_bytes] = runtime
        
    print "\n*********************************\nResults\n******************************" 
    print "Size,Time"
    for key, value in runtimes.iteritems():
        print str(key) + "," + str(value)
            
            
                       
    tc.terminate_cluster()