#!/bin/sh
echo Try to send TERM to HdfsTrafficControl
PID=`jps | grep HdfsTrafficControl | awk '{print $1}'`
echo Process pid is $PID

if [ -z $PID ]; then
  echo Cannot found pid of HdfsTrafficControl.
  jps
  exit 0
fi

kill -TERM $PID
sleep 2
jps
