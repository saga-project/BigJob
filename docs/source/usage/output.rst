#####################
Interpreting Output
#####################

This section explains how to find and work with the output from your BigJob scripts.

It explains the output using the default scripts and default directory names. Please note that the actual directory names can be changed in your scripts.

**Steps to interpreting your output:**

1.) :code:`cd` into your "working directory." In the examples on this tutorial, this would be the :code:`$HOME/agent` directory.

2.) The agent directory contains a directory that is named after the Pilot Service. This directory begins with :code:`bj-####`, where ### is a unique identifier. :code:`cd` into the :code:`bj-####` directory.

3.) If you do a :code:`ls` in this directory, you should see a number of directories that begin with :code:`sj-###` where ### is a unique identifier. These sj-directories represent each compute unit that is associated with the pilot-service. For example, if you ran 4 'single' jobs, you will have 4 sj-directories.

4.) Contained within each sj-directory is a stdout.txt (which contains the output of your executable) and stderr.txt (which contains the output of standard error on the command line -- it will be blank if there were no errors when running your executable) file. Please note that if you defined different names for the "output" and "error" fields in your compute unit description, then the names stdout and stderr will vary.
