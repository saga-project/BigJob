#!/usr/bin/env python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, Ole Christian Weidner"
__license__   = "MIT"

import sys
import getpass
import BigJob.saga as saga

def run(url1, url2, username, queue, project):
    """Test if we can execute a remote bash script via 'bash -c'
    """
    try:
        ctx = saga.Context()
        ctx.context_type = saga.Context.SSH
        ctx.user_id  = username # like 'ssh username@host ...'

        js1 = saga.job.Service(url1)
        js2 = saga.job.Service(url2)
        js1.session.contexts.append(ctx)
        js2.session.contexts.append(ctx)

        # describe our job
        jd = saga.job.Description()

        jd.queue   = queue
        jd.project = project
        jd.wall_time_limit = 2 # minutes
    
        # environment, executable & arguments
        jd.environment = {'MYOUTPUT':'"Hello from BigJob"'}       
        jd.executable  = '/bin/echo'
        jd.arguments   = ['$MYOUTPUT']

        # output options
        jd.output = "BigJob_job.09.stdout" 
        jd.error  = "BigJob_job.09.stderr"

        # create the job (state: New)
        myjob1 = js1.create_job(jd)
        myjob2 = js2.create_job(jd)


        print "Job 1 ID    : %s" % (myjob1.job_id)
        print "Job 1 State : %s" % (myjob1.get_state())
        print "Job 2 ID    : %s" % (myjob2.job_id)
        print "Job 2 State : %s" % (myjob2.get_state())


        print "\n...starting job...\n"
        # run the job (submit the job to PBS)
        myjob1.run()
        myjob2.run()

        print "Job 1 ID    : %s" % (myjob1.job_id)
        print "Job 1 State : %s" % (myjob1.get_state())
        print "Job 2 ID    : %s" % (myjob2.job_id)
        print "Job 2 State : %s" % (myjob2.get_state())

        print "\n...waiting for job...\n"
        # wait for the job to either finish or fail
        myjob1.wait()
        myjob2.wait()

        print "Job 1 State : %s" % (myjob1.get_state())
        print "Job 1 Exitcode  : %s" % (myjob1.exitcode)
        print "Job 2 State : %s" % (myjob2.get_state())
        print "Job 2 Exitcode  : %s" % (myjob2.exitcode)


        failed = False
        why = ""
        if myjob1.get_state() != saga.job.Job.Done:
            failed = True
            why += "Job 1 returned in state 'Failed'. "
            raise(why)

        if myjob2.get_state() != saga.job.Job.Done:
            failed = True
            why += "Job 2 returned in state 'Failed'. "
            raise(why)

    except saga.Exception, ex:
        why = str(ex)

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

        return -1

    print ""
    print "============================================"
    print "The job seems to have executed successfully!"
    print "============================================"
    print "                                            "

    return 0


def usage():
    print 'Usage: python %s ' % __file__
    print '                <URL1>'
    print '                <URL2>'
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
        js_url1 = args[0]
        js_url2 = args[1]

    try:
        outprefix      = args[1]
        remoteusername = args[2]
        queue          = args[3]
        project        = args[4]
    except IndexError:
        pass 

    return run(js_url1, js_url2, remoteusername, queue, project)


if __name__ == '__main__':
    sys.exit(main())
