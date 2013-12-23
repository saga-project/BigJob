#!/usr/bin/env python
from bigjob import logger

import httplib2
import os
import uuid
import time
import saga
import sys
import threading
import json

try:
    import mesos
    import mesos_pb2
except:
    logger.debug("Mesos libraries NOT found")

# For Testing
TOTAL_TASKS = 1
TASK_CPUS = 2
TASK_MEM = 188.0

class MesosStates:
    TASK_FINISHED="TASK_FINISHED"
    TASK_RUNNING="TASK_RUNNING"
    TASK_LOST="TASK_LOST"
    TASK_FAILED = "TASK_FAILED"
    TASK_KILLED = "TASK_KILLED"
    TASK_STAGING = "TASK_STAGING"
    TASK_STARTING = "TASK_STARTING"
    DRIVER_ABORTED = 3
    DRIVER_NOT_STARTED = 1
    DRIVER_RUNNING = 2
    DRIVER_STOPPED = 4
    
class Service(object):
    """ Plugin for Mesos
    
        Manages endpoint in the form of:
        
            mesos://<resource manager>:<port>    
    """

    def __init__(self, resource_url=None, pilot_compute_description=None):
        """Constructor"""
        self.resource_url = resource_url
        self.pilot_compute_description =  pilot_compute_description   
    
    def create_job(self, job_description=None):
        j = Job(job_description, self.resource_url, self.pilot_compute_description)
        return j
            
    
    def __del__(self):
        pass
    
    
    

class Job(object):
    """ Job class for managing a Mesos SchedulerDriver """

    def __init__(self, job_description=None, resource_manager_url=None, pilot_compute_description=None):
        self.job_description = job_description
        self.saga_url = saga.Url(resource_manager_url) 
        self.mesos_url = self.saga_url.host +":" + str(self.saga_url.port)
        self.pilot_compute_description = pilot_compute_description
        self.id="bigjob-" + str(uuid.uuid1())
        self.driver = None
    
    
    def run(self):
        self.thread = threading.Thread(target=self.__run_mesos_scheduler, args=())
        self.thread.daemon = True
        self.thread.start()
        
    
    def get_state(self):
        pass

    
    def cancel(self):
        if self.driver!=None:
            self.driver.stop();             
        self.thread.join()
        
        
    def __run_mesos_scheduler(self):
        logger.debug("Connect to Mesos: %s"%self.mesos_url)
        executor = mesos_pb2.ExecutorInfo()
        executor.executor_id.value = "default"
        #print os.path.abspath("./mesos_executor")
        executor.command.value = "/root/shares/workspace-saga/bigjob/BigJob/bigjob/job_plugin/mesos/mesos_executor"
        #executor.task.data=  json.loads(task.data) if task.data else {}
        executor.name = "BigJob Executor (Python)"
        executor.source = "python_test"
        
        framework = mesos_pb2.FrameworkInfo()
        framework.user = "" # Have Mesos fill in the current user.
        framework.name = "BigJob Framework"
        
        self.driver = mesos.MesosSchedulerDriver(
              BigJobScheduler(executor, self.pilot_compute_description),
              framework,
              self.mesos_url)
        
        self.status = 0 if self.driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        
        



class BigJobScheduler(mesos.Scheduler):
    """
    Mesos schedulers: Implements interface to Mesos master for receiving resource
    offers.
    """
    
    def __init__(self, executor, pilot_compute_description):
        self.executor = executor
        self.taskData = {}
        self.tasksLaunched = 0
        self.tasksFinished = 0
        self.messagesSent = 0
        self.messagesReceived = 0
        self.pilot_compute_description=pilot_compute_description
    
    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID %s" % frameworkId.value
    
    def resourceOffers(self, driver, offers):
        print "Got %d resource offers" % len(offers)
        for offer in offers:
            tasks = []
            print "Got resource offer %s: %s" %(offer.id.value, str(offer))
            if self.tasksLaunched < TOTAL_TASKS:
                tid = self.tasksLaunched
                self.tasksLaunched += 1
                
                print "Accepting offer on %s to start task %d" % (offer.hostname, tid)
                
                task = mesos_pb2.TaskInfo()
                task.task_id.value = str(tid)
                task.slave_id.value = offer.slave_id.value
                task.name = "task %d" % tid
                task.data = json.dumps(self.pilot_compute_description)
                task.executor.MergeFrom(self.executor)
                
                cpus = task.resources.add()
                cpus.name = "cpus"
                cpus.type = mesos_pb2.Value.SCALAR
                cpus.scalar.value = TASK_CPUS
                
                mem = task.resources.add()
                mem.name = "mem"
                mem.type = mesos_pb2.Value.SCALAR
                mem.scalar.value = TASK_MEM
                
                disk = task.resources.add()
                disk.name = "disk"
                disk.type = mesos_pb2.Value.SCALAR
                disk.scalar.value = 400
                                
                tasks.append(task)
                self.taskData[task.task_id.value] = (
                    offer.slave_id, task.executor.executor_id)
            
                driver.launchTasks(offer.id, tasks)
    
    def statusUpdate(self, driver, update):
        print "Task %s is in state %d" % (update.task_id.value, update.state)
        
        # Ensure the binary data came through.
        if update.data != "data with a \0 byte":
            print "The update data did not match!"
            print "  Expected: 'data with a \\x00 byte'"
            print "  Actual:  ", repr(str(update.data))
            #sys.exit(1)
        
        if update.state == mesos_pb2.TASK_FINISHED:
            self.tasksFinished += 1
            if self.tasksFinished == TOTAL_TASKS:
                print "All tasks done, waiting for final framework message"
        
            slave_id, executor_id = self.taskData[update.task_id.value]
            
            self.messagesSent += 1
            driver.sendFrameworkMessage(executor_id, slave_id, 'data with a \0 byte')
    
    
    def frameworkMessage(self, driver, executorId, slaveId, message):
        self.messagesReceived += 1
        
        # The message bounced back as expected.
        if message != "data with a \0 byte":
            print "The returned message data did not match!"
            print "  Expected: 'data with a \\x00 byte'"
            print "  Actual:  ", repr(str(message))
            sys.exit(1)
        print "Received message:", repr(str(message))
        
        if self.messagesReceived == TOTAL_TASKS:
            if self.messagesReceived != self.messagesSent:
                print "Sent", self.messagesSent,
                print "but received", self.messagesReceived
                sys.exit(1)
            print "All tasks done, and all messages received, exiting"
            driver.stop()


 

if __name__ == "__main__":
    mesos_service = Service(resource_url="mesos://172.16.8.197:5050", pilot_compute_description=None)
    job = mesos_service.create_job()
    job.run()
