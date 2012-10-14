#!/usr/bin/env python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, Ole Christian Weidner"
__license__   = "MIT"

import sys
import getpass
import bigjob.saga as saga

def run(url, username, queue, project):
    """Tests if a plug-in can handle both, username
       as part of the URL (i.e., oweidner@host) and
       username as part of a context.
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
        jd.environment = {'MYOUTPUT':'"Hello from BigJob"'}       
        jd.executable  = '/bin/echo'
        jd.arguments   = ['$MYOUTPUT']

        # output options
        jd.output = "BigJob_job.01b.stdout"
        jd.error  = "BigJob_job.01b.stderr"

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
            why = "Job 1 returned in state 'Failed'."

        #######################################################

        url2 = saga.Url(url)
        url2.username = username

        js2 = saga.job.Service(url2)
        myjob2 = js.create_job(jd)

        print "Job ID    : %s" % (myjob2.job_id)
        print "Job State : %s" % (myjob2.get_state())

        print "\n...starting job...\n"
        # run the job (submit the job to PBS)
        myjob2.run()

        print "Job ID    : %s" % (myjob2.job_id)
        print "Job State : %s" % (myjob2.get_state())

        print "\n...waiting for job...\n"
        # wait for the job to either finish or fail
        myjob2.wait()

        print "Job State : %s" % (myjob2.get_state())
        print "Exitcode  : %s" % (myjob2.exitcode)

        failed = False
        why = ""
        if myjob2.get_state() != saga.job.Job.Done:
            failed = True
            why = "Job 2 returned in state 'Failed'."



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
      print ""

    return failed


def usage():
    print 'Usage: python %s ' % __file__
    print '                <URL>'
    print '                <REMOTEUSERNAME>'
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
        remoteusername = args[1]
    try:
        queue = args[2]
        project = args[3]
    except IndexError:
        pass 

    return run(js_url, remoteusername, queue, project)


if __name__ == '__main__':
    sys.exit(main())
