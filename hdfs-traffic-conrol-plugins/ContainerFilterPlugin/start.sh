#/bin/sh

cd ~/ContainerFilterPlugin

if [ "$1" ]; then
  MINRATE="-rate $1"
fi

if [ "$2" ]; then
  TRACEMODE="-trace"
fi

#mvn exec:java -nsu

mvn exec:java -Dexec.mainClass="cntic.process.BuildProcessTree" -Dexec.args="$TRACEMODE $MINRATE" -nsu

