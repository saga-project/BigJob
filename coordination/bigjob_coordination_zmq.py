'''
Encapsulates coordination and communication specifics of bigjob
'''

import logging
import threading
import datetime
import sys
import os
import pickle
import pdb
import zmq
import traceback
import Queue
import socket
import time
from zmq.eventloop import ioloop, zmqstream
import zlib, cPickle as pickle
from bigjob import logger

if sys.version_info < (2, 5):
    sys.path.append(os.path.dirname( os.path.abspath( __file__) ) + "/../ext/uuid-1.30/")
    sys.stderr.write("Warning: Using unsupported Python version\n")
    
logging.debug(str(sys.path))
import uuid

SERVER_IP="localhost"
SERVER_PORT=0

NUMBER_RETRIES=2

class message:    
    def __init__(self, command, key, value):
        self.command = command
        self.key = key
        self.value = value

    def __repr__(self):
        return ("command: %s, key: %s, value: %s "%(self.command, self.key, self.value))

class bigjob_coordination(object):
    '''
    Encapsulates communication and coordination
    Implementation based on ZMQ 
    '''
    def __init__(self, server=SERVER_IP, server_port=SERVER_PORT, server_connect_url=None,
                 username=None, password=None, dbtype=None, url_prefix=None):
        '''
        Constructor
        set server and server_port to create a service (server)
        set server_connect_url to connect to a service (client)
        '''  
        self.stopped = False
        self.has_stopped=False        
        
        
        # state managed by server
        self.pilot_states = {}
        self.job_ids = []
        self.jobs = {}
        self.job_states = {}
        self.new_job_queue = Queue.Queue()
        
        # Lock for server and client to manage concurrent access
        self.resource_lock = threading.Lock()
        
        # Client side queue
        self.subjob_queue = Queue.Queue()
            
        # set up ZMQ client / server communication
        self.context = zmq.Context()
       
        self.server_role = False  
        self.address = None 
        self.dbtype = ""   
        if server == "*":
            server = socket.gethostname()        
        logging.debug("Server: " + server)         
        if server_connect_url==None: # role = Server
            self.server_role = True
            # start eventloop
            self.startup_condition = threading.Condition()
            self.eventloop_thread=threading.Thread(target=self.__server, args=(server, server_port))
            self.eventloop_thread.daemon=True
            self.eventloop_thread.start()
            
            logging.debug("Setting up socket for notifications")
            # socket for sending notification
            self.push_socket = self.context.socket(zmq.PUSH)
            push_port = self.push_socket.bind_to_random_port("tcp://*")    
            self.push_address = "tcp://"+server+":"+str(push_port)                
            
            
            logging.debug("Waiting for server to complete startup")
            # wait for server thread to complete startup
            self.startup_condition.acquire()
            while self.address == None:
                self.startup_condition.wait()
            self.startup_condition.release()                       
        else: # role client
            urls = server_connect_url.split(",")
            self.address = urls[0]
            self.push_address = urls[1]           
            self.server_role = False
            self.pilot_url = server_connect_url
            
           
            
        
        logging.debug("Connect sockets to server: " + self.address + " push: " + self.push_address)
        # connect to REP server
        self.client_socket = self.context.socket(zmq.REQ)
        self.client_socket.connect(self.address)
        
        # connect to PUSH server
        self.pull_socket = self.context.socket(zmq.PULL)
        self.pull_socket.connect(self.push_address)
        
        if self.server_role==False:
            self.notification_thread=threading.Thread(target=self.__wait_for_notifications)
            self.notification_thread.daemon=True
            self.notification_thread.start()
        
        
        logging.debug("Connected to REP socket at: " + self.address + " and PUSH socket at: " + self.push_address)
        logging.debug("C&C ZMQ system initialized")
        

        
    def get_address(self):
        """ return handle to c&c subsystems """
        return self.address+"," +self.push_address
        
    #####################################################################################
    # Pilot-Job State
    def set_pilot_state(self, pilot_url, new_state, stopped=False):     
        logging.debug("BEGIN update state of pilot job to: " + str(new_state) 
                       + " Lock: " + str(self.resource_lock))
        counter = 0
        result = None
        while result != "SUCCESS" and counter < NUMBER_RETRIES:
            with self.resource_lock:  
                msg = message("set_pilot_state", pilot_url, {"state":str(new_state), "stopped":str(stopped)})
                try:
                    self.client_socket.send_pyobj(msg, zmq.NOBLOCK)        
                    result = self.client_socket.recv_pyobj()
                except:
                    traceback.print_exc(file=sys.stderr)
            # stop background thread running the server (if True)
            self.stopped=stopped             
            if result == None:
                logging.error("RETRY set_pilot_state")
                counter = counter + 1
                time.sleep(1) 
        logging.debug("END update state of pilot job to: " + str(new_state))
        
    def get_pilot_state(self, pilot_url):
        logging.debug("BEGIN get_pilot_state: %s lock: %s" % (pilot_url, str(self.resource_lock)))
        counter = 0
        result = None
        while result ==None and counter < NUMBER_RETRIES:
            with self.resource_lock:   
                msg = message("get_pilot_state", pilot_url, " Lock: ")
                try:
                    self.client_socket.send_pyobj(msg, zmq.NOBLOCK)
                    result = self.client_socket.recv_pyobj()
                except:
                    pass
                  
            logging.debug("END get_pilot_state: %s state: %s, lock: %s" % 
                          (pilot_url, str(result.value), str(self.resource_lock)))
            if result == None:               
                counter = counter + 1
                logging.error("RETRY get_pilot_state - Retry # %d"%counter)
                time.sleep(1) 
        return result.value
        logging.debug("END get_pilot_state: %s lock: %s" % (pilot_url, str(self.resource_lock)))
    
    def get_jobs_of_pilot(self, pilot_url):
        """ returns array of job_url that are associated with a pilot """
        """ local only - used only by manager """
        return self.job_ids
    
    def delete_pilot(self, pilot_url):
        """ local only - used only by manager """
        # stop everything
        self.stopped=True
        msg = message("STOP", pilot_url, "")        
        self.client_socket.send_pyobj(msg, zmq.NOBLOCK)
        if self.server_role == True:
            self.push_socket.send_pyobj(msg, zmq.NOBLOCK)
        #self.eventloop_thread.join()
        logging.debug("Has stopped: " + str(self.has_stopped))
        self.__shutdown()
        
    
    #####################################################################################
    # Sub-Job State    
    def set_job_state(self, job_url, new_state):
        logging.debug("Set job state: %s to %s"%(job_url, new_state))
        counter = 0
        result = None
        while result == None and counter < NUMBER_RETRIES:
            with self.resource_lock:   
                msg = message("set_job_state", job_url, new_state)
                try:
                    self.client_socket.send_pyobj(msg, zmq.NOBLOCK)
                    result = self.client_socket.recv_pyobj()
                except:
                    traceback.print_exc(file=sys.stderr)
      
            if result == None:
                counter = counter + 1
                logging.error("RETRY %d set_job_state %s to %s"%(counter, job_url, new_state))
                if counter == NUMBER_RETRIES-1:
                    self.__reset_client_socket()              
                time.sleep(2)
                continue # retry
            else:
                logging.debug("SUCCESS set_job_state (%s to %s)"%(job_url, new_state))
            
        
        
    def get_job_state(self, job_url):
        #logging.debug("get_job_state")
        with self.resource_lock:   
            msg = message("get_job_state", job_url, "")
            self.client_socket.send_pyobj(msg, zmq.NOBLOCK)
            result = self.client_socket.recv_pyobj()            
        return result.value      
    
    #####################################################################################
    # Pilot-Job Description
    def set_pilot_description(self, pilot_url, description):     
        pass
    
    def get_pilot_description(self, pilot_url):
        pass
    
    #####################################################################################
    # Sub-Job Description
    def set_job(self, job_url, job_dict):        
        """ local only - used only by manager """
        self.job_ids.append(job_url)
        self.jobs[job_url] = job_dict   
        self.job_states[job_url]="Unknown"     
    
    def get_job(self, job_url):       
        if self.jobs.has_key(job_url)==False:
            logging.debug("get_job: " + str(self.resource_lock))
            with self.resource_lock: 
                logging.debug("get_job (lock acquired): " + str(self.resource_lock))
                msg = message("get_job", job_url, "")
                self.client_socket.send_pyobj(msg)
                result = self.client_socket.recv_pyobj()
                self.jobs[job_url] = result.value
                logging.debug("received job: "  + str(result.value))
        return self.jobs[job_url] 
    
    def delete_job(self, job_url):
        self.job_ids.remove(job_url)
        del self.jobs[job_url]
        del self.job_ids[job_url]
    
    
    #####################################################################################
    # Distributed queue for sub-jobs
    def queue_job(self, pilot_url, job_url):        
        if self.server_role == False: # just re-queue locally at client
            self.subjob_queue.put(job_url)        
        elif self.server_role == True:
            """ queue new job to pilot """
            logging.debug("queue_job " + str(self.resource_lock))
            counter = 0
            result = None
            success = False
            while result ==None and counter < NUMBER_RETRIES:
                with self.resource_lock:   
                    msg = message("queue_job", "", job_url)
                    try:
                        self.client_socket.send_pyobj(msg, zmq.NOBLOCK)
                        result = self.client_socket.recv_pyobj()
                        success=True
                    except:
                        traceback.print_exc(file=sys.stderr)     
                if result == None:
                    counter = counter + 1
                    logging.error("RETRY %d queue_job"%counter)
                    if counter == NUMBER_RETRIES and success==False:
                        self.__reset_client_socket()
                    time.sleep(2)
            msg2 = message("notification", "", job_url)
            self.push_socket.send_pyobj(msg2)
        
        return success             
             
        
    def dequeue_job(self, pilot_url):
        """ dequeue to new job  of a certain pilot """
        return self.subjob_queue.get()
        
    
    
    
    #####################################################################################
    # Private functions    
    def __server_handler(self, messageList):
        """ server for managing job / pilot job states via ZMQ """
        msg = pickle.loads(messageList[0])
        logging.debug("Message received: " + str(msg))
        self.__handle_message(msg, self.stream)
        
    
    def __handle_message(self, msg, reply_socket):
        try:
            command = msg.command        
            if command == "set_pilot_state":
                self.pilot_states[msg.key] = msg.value
                reply_socket.send_pyobj("SUCCESS")
                #self.service_socket.send("")            
            elif command == "get_pilot_state":
                result = message ("", "", self.pilot_states[msg.key])
                reply_socket.send_pyobj(result, zmq.NOBLOCK)                
            elif command == "set_job_state":
                self.job_states[msg.key] = msg.value
                reply_socket.send_pyobj("SUCCESS", zmq.NOBLOCK)       
            elif command == "get_job_state":
                result=message("", "", self.job_states[msg.key])
                reply_socket.send_pyobj(result, zmq.NOBLOCK)            
            elif command == "get_job":
                result = message("","", self.jobs[msg.key])
                reply_socket.send_pyobj(result, zmq.NOBLOCK)
            elif command == "queue_job":                
                self.new_job_queue.put(msg.value)
                reply_socket.send_pyobj("SUCCESS", zmq.NOBLOCK)                       
            elif command == "dequeue_job":
                new_job=None
                try:
                    new_job = self.new_job_queue.get(False)
                except:                
                    pass
                result = message("","", new_job)
                reply_socket.send_pyobj(result, zmq.NOBLOCK)
            else:
                logging.debug("sending default reply")
                reply_socket.send_pyobj("", zmq.NOBLOCK)
        except:
            traceback.print_exc(file=sys.stderr)
    
    
    def __server(self, server, server_port):
        """ server for managing job / pilot job states via ZMQ """
        service_socket = self.context.socket(zmq.REP)
        if SERVER_PORT==0: # random port
            server_port = service_socket.bind_to_random_port("tcp://*")    
            self.address = "tcp://"+server+":"+str(server_port)                
        elif server == "localhost":
            self.server_address = "tcp://*:"+str(server_port)
            self.address = "tcp://"+server+":"+str(server_port)
            service_socket.bind(self.server_address)
        else:
            self.server_address = "tcp://"+server+":"+str(server_port)
            self.address = self.server_address
            service_socket.bind(self.server_address)
        logging.debug("Starting service at: " + self.address)        
        self.startup_condition.acquire()
        self.startup_condition.notifyAll()   
        self.startup_condition.release()
        logging.debug("Startup condition signaled")
        while self.stopped == False:
            #logging.debug("Waiting for messages...")
            try:
                msg = service_socket.recv_pyobj()            
                #logging.debug("Message received: " + str(msg))
                self.__handle_message(msg, service_socket)
            except:
                pass
            #logging.debug("Message handled: " + str(msg) + " stopped = " + str(self.stopped))
            #pdb.set_trace()
        logging.debug("__server thread stopped: " + str(self.stopped))
        self.has_stopped = True
        #service_socket.close()
            
    def __loop(self):
        self.loop = ioloop.IOLoop.instance()
        #self.loop.add_handler(self.service_socket, self.__server_handler, zmq.POLLIN)
        self.stream = zmqstream.ZMQStream(self.service_socket, self.loop)
        self.stream.on_recv(self.__server_handler)
        logging.debug("Start event loop")
        self.loop.start()
        
    def __reset_client_socket(self):
        logging.error("RESETING client socket")
        with self.resource_lock: 
            try:
                self.client_socket.close()
            except:
                traceback.print_exc(file=sys.stderr)
            self.client_socket = self.context.socket(zmq.REQ)
            self.client_socket.connect(self.address)
        
    def __wait_for_notifications(self):
        """ waits for notifications and puts new jobs into queue """    
        #while result==None and counter < NUMBER_RETRIES:
        while self.stopped == False:
            # read object from queue
            logging.debug(" __wait_for_notifications: polling for new jobs - stopped: " + str(self.stopped))
            msg = message ("dequeue_job", self.pilot_url, "")
            try:
                with self.resource_lock: 
                    self.client_socket.send_pyobj(msg, zmq.NOBLOCK)
                    result = self.client_socket.recv_pyobj().value
                logging.debug(" __wait_for_notifications: received new jobs " + str(result))
                if result != None:
                    self.subjob_queue.put(result)
                    time.sleep(0.2)
                    continue                                              
            except:                
                traceback.print_exc(file=sys.stderr)                    
                logging.error("Error dequeuing job")                  
                time.sleep(1)
                continue   
            logging.debug(" __wait_for_notifications: End Loop - stopped " + str(self.stopped))
                
            #if counter == NUMBER_RETRIES-1 and success == False:
            #    self.__reset_client_socket()              
            #    time.sleep(2)
            #    continue # retry
            
            #if counter == NUMBER_RETRIES and success == False:
            #    return result
            
            logging.debug(" __wait_for_notifications: wait for notification")
            # wait for next job notification
            if result == None:
                try:
                    logging.debug("wait for notification")
                    self.pull_socket.recv_pyobj()
                    logging.debug("received notification")
                except:
                    pass
        
        # wait for next job notification
        #while self.stopped == False:
        #    logging.debug("wait for notification")
        #    self.pull_socket.recv_pyobj()
        #    logging.debug("received notification")
            
    def __shutdown(self):
        logging.debug("shutdown ZMQ")
        try:     
            #self.client_socket.close()            
            #self.service_socket.close()
            #self.context.term()
            pass
        except:
            pass       
            
            