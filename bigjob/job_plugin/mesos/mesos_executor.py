#!/usr/bin/env python
import threading
import os, sys
import logging
import bigjob
import bigjob.bigjob_agent
import json
import time
try:
    import mesos
    import mesos_pb2
except:
    logging.error("Mesos libraries NOT found")


class BigJobExecutor(mesos.Executor):
    """
    Mesos executor implementation that runs BigJob. 
    """
    
    def launchTask(self, driver, task):
        print "Start Big Job Thread"
        thread = threading.Thread(target=run_bigjob, args=(driver, task))
        thread.daemon = True
        thread.start()
        print "Start Thread for launching BigJob"
        #thread.join()

    def frameworkMessage(self, driver, message):
        # Send it back to the scheduler.
        driver.sendFrameworkMessage(message)


# This function is called in its own thread to actually run the user's command.
# When it finishes, it shuts down the scheduler driver (disconnecting the 
# framework) and exits the program.
def run_bigjob(driver, task, args=[]):    
    print "Running BigJob as Mesos Task: %s" % task.task_id.value
    update = mesos_pb2.TaskStatus()
    update.task_id.value = task.task_id.value
    update.state = mesos_pb2.TASK_RUNNING
    update.data = 'data with a \0 byte'
    driver.sendStatusUpdate(update)
    
    data = json.loads(task.data) if task.data else {}
    print str(data)       
    try:
        code = os.system("/bin/date")
        print "Command completed with code %d" % code
        args = ("bigjob_agent.py", 
                data["coordination_host"], 
                data["pilot_url"], 
                data["external_queue"])
        bigjob.bigjob_agent.bigjob_agent(args)
        while 1:
            time.sleep(60) # Mesos process control will automatically kill BigJob Agent
        print "Completed Run of BigJob Agent."
    except OSError,e:
        print "os.system call failed, see stderr for details"
        driver.stop()
        sys.exit(2)
        
    print "Send Task finished."
    update = mesos_pb2.TaskStatus()
    update.task_id.value = task.task_id.value
    update.state = mesos_pb2.TASK_FINISHED
    update.data = 'data with a \0 byte'
    driver.sendStatusUpdate(update)            
    driver.stop()
    sys.exit(0)

 
if __name__ == "__main__":
    print "Starting BigJob executor"
    driver = mesos.MesosExecutorDriver(BigJobExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)     
