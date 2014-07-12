 
 
 Setup Tachyon
 
 <http://tachyon-project.org/v0.4.1/Configuration-Settings.html>
 <http://tachyon-project.org/v0.4.1/Running-Tachyon-on-a-Cluster.html>
 
 
 Download:
 
 
 Configure
 
 	 cd conf
 	 cp tachyon-env.sh.template tachyon-env.sh
 	 vi conf/slaves
 
 Run:
 
 	tachyon-start.sh all NoMount
 
 	tachyon runTest Basic ASYNC_THROUGH
 
 	telnet c546-504.stampede.tacc.utexas.edu 19999
 
 
 Stop
 
 	tachyon-stop.sh 
 
 
 Test
 
 	tachyon tfs copyFromLocal ~/myMPI.o79503 /test/test.txt