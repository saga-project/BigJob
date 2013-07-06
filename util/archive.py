# Archive Redis content and generate some primitive analytics
#
# On OS X 
# brew install zeromq freetype
# pip install pyzmq tornado pandas ipython matplotlib 

import redis
import os, sys
import pandas as pd


from bigjob import logger

# Archive the following redis urls
REDIS_URLS=["redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379", "redis://localhost"]


REDIS_SERVER="localhost"
REDIS_SERVER_PORT=6379
REDIS_URL_SCHEME="redis://"

class RedisDownloader(object):
    
    def __init__(self, redis_url):
        '''
        Constructor
        '''
        server_port=6379
        self.redis_url=redis_url  
        self.password=None  
        start_index =  self.redis_url.find(REDIS_URL_SCHEME)+len(REDIS_URL_SCHEME)
        server_and_port = self.redis_url[start_index:]
        password_end = server_and_port.find("@") 
        # parse out password
        if password_end != -1:
            self.password = server_and_port[:password_end]
            start_index=password_end
            server_and_port= server_and_port[(password_end+1):]
            
        # port and hostname
        if server_and_port.find(":")==-1:
            server=server_and_port
            server_port = REDIS_SERVER_PORT
        else:
            server = server_and_port.split(":")[0]
            server_port = int(server_and_port.split(":")[1])
        
        logger.debug("Connect to Redis: " + server + " Port: " + str(server_port))
        
        if self.password==None:
            self.redis_client = redis.Redis(host=server, port=server_port, db=0)
        else:
            self.redis_client = redis.Redis(host=server, port=server_port, password=self.password, db=0)

        self.pipe = self.redis_client.pipeline()
        try:
            self.redis_client.ping()
        except:
            logger.error("Please start Redis server!")
            raise Exception("Please start Redis server!")
        
        
    def get_pilots(self):
        pilots = self.redis_client.keys("bigjob:bj-*")    
        for i in pilots:
            if ":jobs:" not in i and i.count(":")==2:
                #print i
                self.pipe.hgetall(i)
        response = self.pipe.execute()
        return response;
    
    
    def get_cus(self):
        cus = self.redis_client.keys("*:jobs:*")    
        for i in cus:
            self.pipe.hgetall(i)
        response = self.pipe.execute()
        return response;
        

if __name__ == '__main__':
    if len(sys.argv)>1:
        print "Get data from " + sys.argv[1]
        rd = RedisDownloader(sys.argv[1])
        pilots = rd.get_pilots()
        cus = rd.get_cus()
        print "Loaded Redis data: %d pilots, %d cus"%(len(pilots), len(cus))
    else:
        for i in REDIS_URLS:
            rd = RedisDownloader(i)
            pilots = rd.get_pilots()
            cus = rd.get_cus()
        
        
        
        
        