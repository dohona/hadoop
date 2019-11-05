export CLASSPATH=`yarn classpath`
java -cp $CLASSPATH  org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.HdfsTrafficControl

