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
import urlparse
import pwd
import math
import pdb

from webhdfs.webhdfs import WebHDFS 

 
#HDFS_URL="hdfs://localhost:50070"
HDFS_URL="http://c520-303:50070"
RESULT_FILE_PREFIX="hdfs-inmem"
RESULT_DIR="results"

MIN_SIZE=28 # 2**28 bytes
MAX_SIZE=36 # 2**29 bytes

class HDFSClusterManager():
    
    def __init__(self, nodes=[]):
        self.client =  WebHDFS(self.host, self.port, self.user)
        
    def create_cluster(self):
        pass    
            
            
    def terminate_cluster(self):
        pass


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    if size>268435456:
        s = '%030x' % random.randrange(long(16L**268435456))
        for i in range(28, int(math.log(size, 2))):
           s = s + s 
    else:
        s = '%030x' % random.randrange(long(16L**size))
    return s

def id_generator_file(size=6, chars=string.ascii_uppercase + string.digits, filename=None):
    f = open(filename, "w")
    if size>268435456:
        s = '%030x' % random.randrange(long(16L**268435456))
        # Base Unit of Data written to disk is 2^28 = 268,435,456 bytes = 256 MB
        #for i in range(28, int(math.log(size, 2))):
        # how often do we need to write 256 MB
        number_of_writes = size/pow(2, 28) 
        for j in range(0, number_of_writes):
            f.write(s)
    else:
        s = '%030x' % random.randrange(long(16L**size))
        f.write(s)
    f.close()


def id_generator_file_line(size=6, chars=string.ascii_uppercase + string.digits, filename=None):
    f = open(filename, "w")
    n = size/1024
    for i in range(0, n):
        s = '%030x' % random.randrange(long(16L**1024))+"\n"
        f.write(s)
        #f.write(''.join(random.choice(chars) for _ in range(1023)) + "\n")
    f.close()

def get_number_hadoop_nodes(fname="/etc/hadoop/conf/slaves"):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1
    
def test_without_caching(number_of_nodes, number_replicas, f, client):
    client.rmdir("/tmp/test")
    client.rmdir("/tmp/out")
    client.mkdir("/tmp/test/")
    runtimes = {}
    for i in range(MIN_SIZE, MAX_SIZE):
        num_bytes = 2 ** i
        print "PUT File Size: %s MB" % str(num_bytes / 1024 / 1024)
        s = id_generator(num_bytes)
        print "Set string with len: " + str(len(s)) + " size of: " + str(sys.getsizeof(s))
        start = time.time()
        client.put(s, "/tmp/test/test_" + str(num_bytes), number_replicas)
        runtime = time.time() - start
        runtimes[num_bytes] = runtime
    
    print "\n*********************************\nResults\n******************************"
    print "Size, Time, Backend, NumNodes, NumInstances, Type, NumReplicas "
    for key, value in runtimes.iteritems():
        result = str(key) + "," + str(value) + ",HDFS," + str(number_of_nodes) + "," + str(number_of_nodes) + ",write," + str(number_replicas)
        print result
        f.write(result + "\n")
    
    for repeat in range(0, 10):
        runtimes = {}
        for i in range(MIN_SIZE, MAX_SIZE):
            num_bytes = 2 ** i
            print "GET File Size: %s MB" % str(num_bytes / 1024 / 1024)
            s = id_generator(num_bytes)
            start = time.time()
            s = client.get("/tmp/test/test_" + str(num_bytes))
            runtime = time.time() - start
            runtimes[num_bytes] = runtime
        
        for key, value in runtimes.iteritems():
            result = str(key) + "," + str(value) + ",HDFS," + str(number_of_nodes) + "," + str(number_of_nodes) + ",read," + str(number_replicas) + "," + str(repeat)
            print result
            f.write(result + "\n")


def test_with_caching(number_of_nodes, number_replicas, f, client):
    client.rmdir("/tmp/test")
    client.mkdir("/tmp/test/")
    os.system("sudo -u hdfs hdfs cacheadmin -addPool test")
    runtimes = {}
    for i in range(MIN_SIZE, MAX_SIZE):
        num_bytes = 2 ** i
        print "PUT File Size: %s MB" % str(num_bytes / 1024 / 1024)
        s = id_generator(num_bytes)
        print "Set string with len: " + str(len(s)) + " size of: " + str(sys.getsizeof(s))
        start = time.time()
        filename = "/tmp/test/test_" + str(num_bytes)
        client.put(s, filename, number_replicas)
        runtime = time.time() - start
        runtimes[num_bytes] = runtime
        os.system("sudo -u hdfs hdfs cacheadmin -addDirective -path %s -pool test"%filename)
        
    time.sleep(1)
    print "\n*********************************\nResults\n******************************"
    print "Size, Time, Backend, NumNodes, NumInstances, Type, NumReplicas "
    for key, value in runtimes.iteritems():
        result = str(key) + "," + str(value) + ",HDFS," + str(number_of_nodes) + "," + str(number_of_nodes) + ",write," + str(number_replicas)
        print result
        f.write(result + "\n")
    
    for repeat in range(0, 10):
        runtimes = {}
        for i in range(MIN_SIZE, MAX_SIZE):
            num_bytes = 2 ** i
            print "GET File Size: %s MB" % str(num_bytes / 1024 / 1024)
            s = id_generator(num_bytes)
            start = time.time()
            s = client.get("/tmp/test/test_" + str(num_bytes))
            runtime = time.time() - start
            runtimes[num_bytes] = runtime
        
        for key, value in runtimes.iteritems():
            result = str(key) + "," + str(value) + ",HDFS," + str(number_of_nodes) + "," + str(number_of_nodes) + ",read_cache," + str(number_replicas) + "," + str(repeat)
            print result
            f.write(result + "\n")


def test_with_inmem(number_of_nodes, number_replicas, f, client, cache=True):
    """ Test Hadoop 2.6 Memory capbilities:
        https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ArchivalStorage.html
        https://issues.apache.org/jira/browse/HDFS-5851
    """
    client.rmdir("/tmp/test")
    client.mkdir("/tmp/test/")

    print "\n*********************************\nResults\n******************************"
    print "Size, Time, Backend, NumNodes, NumInstances, Type, NumReplicas "
    for repeat in range(0, 3):
        runtimes = {}
        runtimes_write = {}
        for i in range(MIN_SIZE, MAX_SIZE):
            num_bytes_scenario = 2 ** i
            filename = "/tmp/test/test_" + str(num_bytes_scenario)
            try:
                os.mkdir(os.path.dirname(filename))
            except:
                pass
            print "PUT File Size: %s MB" % str(num_bytes_scenario / 1024 / 1024)
            id_generator_file(num_bytes_scenario, filename=filename)
            print "CREATED FILE with size of: " + str(os.path.getsize(filename))
            num_bytes=os.path.getsize(filename)
            scenario="write"
            if cache:
                command = "hdfs dfs -put -l %s %s"%(filename,filename)
                scenario="write_memory"
            else:
                command = "hdfs dfs -put %s %s"%(filename,filename)
            print "PUT FILE TO HDFS: %s"%command
            start = time.time()
            os.system(command)
            runtime = time.time() - start
            runtimes[num_bytes_scenario] = runtime

            print "GET File Size: %s MB" % str(num_bytes / 1024 / 1024)
            command="hadoop fs -text %s > /dev/null"%(filename)
            print command
            start = time.time()
            #s = client.get("/tmp/test/test_" + str(num_bytes))
            os.system(command)
            runtime = time.time() - start
            runtimes_write[num_bytes_scenario] = runtime
            
            os.system("hadoop fs -rm -r %s"%(filename))
            os.remove(filename)

        time.sleep(1)

        for key, value in runtimes.iteritems():
            result = str(key) + "," + str(value) + ",HDFS," + str(number_of_nodes) + "," + str(number_of_nodes) + "," + scenario + "," + str(number_replicas)+ "," + str(repeat)
            print result
            f.write(result + "\n")
        f.flush()
        scenario_write = scenario.replace("write", "read")
        for key, value in runtimes_write.iteritems():
            result = str(key) + "," + str(value) + ",HDFS," + str(number_of_nodes) + "," + str(number_of_nodes) + "," + scenario_write + "," + str(number_replicas) + "," + str(repeat)
            print result
            f.write(result + "\n")
            f.flush()

def test_with_inmem_mr(number_of_nodes, number_replicas, f, client, cache=True):
    """ Test Hadoop 2.6 Memory capbilities:
        https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ArchivalStorage.html
        https://issues.apache.org/jira/browse/HDFS-5851
    """
    client.rmdir("/tmp/test")
    client.mkdir("/tmp/test/")

    print "\n*********************************\nResults\n******************************"
    print "Size, Time, Backend, NumNodes, NumInstances, Type, NumReplicas "
    for repeat in range(0, 3):
        runtimes = {}
        runtimes_write = {}
        for i in range(MIN_SIZE, MAX_SIZE):
            num_bytes_scenario = 2 ** i
            filename = "/tmp/test/test_" + str(num_bytes_scenario)
            try:
                os.mkdir(os.path.dirname(filename))
            except:
                pass
            print "PUT File Size: %s MB" % str(num_bytes_scenario / 1024 / 1024)
            id_generator_file_line(num_bytes_scenario, filename=filename)
            print "CREATED FILE with size of: " + str(os.path.getsize(filename))
            num_bytes=os.path.getsize(filename)
            scenario="write"
            if cache:
                command = "hdfs dfs -put -l %s %s"%(filename,filename)
                scenario="write_memory"
            else:
                command = "hdfs dfs -put %s %s"%(filename,filename)
            print "PUT FILE TO HDFS: %s"%command
            start = time.time()
            os.system(command)
            runtime = time.time() - start
            runtimes[num_bytes_scenario] = runtime

            print "GET File Size: %s MB with" % str(num_bytes / 1024 / 1024)
            #command="hadoop jar /home1/01131/tg804093/work/hadoop-2.6.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.0.jar grep %s %s %s"%(filename, "/tmp/out", "testtest")
            command="hadoop jar /home1/01131/tg804093/work/hadoop-2.6.0/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar -input %s -output %s -numReduceTasks 0"%(filename, "/tmp/out")
            print command
            start = time.time()
            #s = client.get("/tmp/test/test_" + str(num_bytes))
            os.system(command)
            runtime = time.time() - start
            runtimes_write[num_bytes_scenario] = runtime
            
            os.system("hadoop fs -rm -r %s"%(filename))
            os.system("hadoop fs -rm -r /tmp/out")
            os.system("hadoop fs -expunge")
            os.remove(filename)

        time.sleep(1)
        for key, value in runtimes.iteritems():
            result = str(key) + "," + str(value) + ",HDFS," + str(number_of_nodes) + "," + str(number_of_nodes) + "," + scenario + "," + str(number_replicas)+ "," + str(repeat)
            print result
            f.write(result + "\n")
        f.flush()
        scenario_write = scenario.replace("write", "read")
        scenario_write = scenario_write + "_mr"
        for key, value in runtimes_write.iteritems():
            result = str(key) + "," + str(value) + ",HDFS," + str(number_of_nodes) + "," + str(number_of_nodes) + "," + scenario_write + "," + str(number_replicas) + "," + str(repeat)
            print result
            f.write(result + "\n")


if __name__ == '__main__':
    
    # Preparation and configuration
    slaves=None
    if os.environ.has_key("HADOOP_CONF_DIR"):
        slaves=os.path.join(os.environ["HADOOP_CONF_DIR"], "slaves")
    number_of_nodes = get_number_hadoop_nodes(slaves)
    number_replicas = 3 
    d = datetime.datetime.now()
    result_filename = RESULT_FILE_PREFIX + d.strftime("%Y%m%d-%H%M%S") + ".csv"
    f = open(os.path.join(RESULT_DIR, result_filename), "w")
    f.write("Size, Time, Backend, NumNodes, NumInstances, Type, NumReplicas,Repeat\n")

    u = urlparse.urlparse(HDFS_URL)
    user = pwd.getpwuid(os.getuid())[0]
    client =  WebHDFS(u.hostname, u.port, user)
    
    test_with_inmem_mr(number_of_nodes, number_replicas, f, client,cache=False)
    test_with_inmem_mr(number_of_nodes, number_replicas, f, client,cache=True)
    test_with_inmem(number_of_nodes, number_replicas, f, client,cache=False)
    test_with_inmem(number_of_nodes, number_replicas, f, client, cache=True)
    #test_without_caching(number_of_nodes, number_replicas, f, client)
    #test_with_caching(number_of_nodes, number_replicas, f, client)
    
    f.close()
    os.system("cd /home1/01131/tg804093; /home1/01131/tg804093/clean.sh")
