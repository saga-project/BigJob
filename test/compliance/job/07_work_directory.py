#!/usr/bin/env python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, Ole Christian Weidner"
__license__   = "MIT"

import sys
import getpass
import BigJob.saga as saga

def run(url, workingdir, username, queue, project):
    """Test if we can execute a remote bash script via 'bash -c'
    """
    try:
        ctx = saga.Context()
        ctx.context_type = saga.Context.SSH
        ctx.user_id  = username # like 'ssh username@host ...'

        js = saga.job.Service(url)
        js.session.contexts.append(ctx)

        # describe our job
        jd = saga.job.Description()

        jd.queue   = queue
        jd.project = project
        jd.wall_time_limit = 5 # minutes
    
        # environment, executable & arguments
        jd.working_directory = workingdir
        jd.environment = {'MYOUTPUT':'"Hello from BigJob"'}       
        jd.executable  = '/bin/echo'
        jd.arguments   = ['$MYOUTPUT']

        # output options
        jd.output = "BigJob_job.07.stdout"
        jd.error  = "BigJob_job.07.stderr"

        # create the job (state: New)
        myjob = js.create_job(jd)

        print "Job ID    : %s" % (myjob.job_id)
        print "Job State : %s" % (myjob.get_state())

        print "\n...starting job...\n"
        # run the job (submit the job to PBS)
        myjob.run()

        print "Job ID    : %s" % (myjob.job_id)
        print "Job State : %s" % (myjob.get_state())

        print "\n...waiting for job...\n"
        # wait for the job to either finish or fail
        myjob.wait()

        print "Job State : %s" % (myjob.get_state())
        print "Exitcode  : %s" % (myjob.exitcode)

        failed = False
        why = ""
        if myjob.get_state() != saga.job.Job.Done:
            failed = True
            why = "Job returned in state 'Failed'."

    except saga.Exception, ex:
        failed = True
        why = str(ex)

    if failed == True:
       print ""
       print "============================================"
       print "The job seems to have FAILED!"
       print "============================================"
       print "                                            "
       print "%s" % (why)
       print "Please run this test again with SAGA_VERBOSE=5 "
       print "and report the results at: "
       print ""
       print "https://github.com/saga-project/BigJob/issues\n"

    else: 
      print ""
      print "============================================"
      print "The job seems to have executed successfully!"
      print "============================================"
      print "                                            "
      print "NOW, SOME MANUAL CHECKING IS REQUIRED!      "
      print "                                            "
      print "(1) Login to %s                             " % (url)
      print "(2) Make sure the %s/file BigJob_job.07.stdout exists" % (workingdir)
      print "(3) Make sure %s/BigJob_job.01.stdout contains the string 'Hello from BigJob'" % (workingdir)
      print ""
      print "If (1)-(3) are ok, this test can be considered as PASSED\n"

    return failed


def usage():
    print 'Usage: python %s ' % __file__
    print '                <URL>'
    print '                <WORKINGDIR> (must exist on the target machine)'
    print '                <REMOTEUSERNAME (default: local username)>'
    print '                <QUEUE (default: None)>'
    print '                <PROJECT (default: None)>'

def main():
    remoteusername = getpass.getuser()
    queue = None
    project = None
    js_url = None

    args = sys.argv[1:]
    if len(args) < 2:
        usage()
        sys.exit(-1)
    else:
        js_url = args[0]

    try:
        workingdir     = args[1]
        remoteusername = args[2]
        queue          = args[3]
        project        = args[4]
    except IndexError:
        pass 

    return run(js_url, workingdir, remoteusername, queue, project)


if __name__ == '__main__':
    sys.exit(main())
