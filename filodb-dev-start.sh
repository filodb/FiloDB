#!/usr/bin/env bash
set -e
#set -x

function showHelp {
        echo "`basename $0` [-h] [-d] [-o arg]"
        echo "   -h help"
        echo "   -d start downsample server"
        echo "   -o ordinal number of this dev server"
        echo "configuration used is hard-coded to conf/timeseries-filodb-server.conf"
}

ORDINAL="0"
DOWNSAMPLE=""
while getopts "ho:d" opt; do
    case "$opt" in
    h|\?) showHelp
        exit 1
        ;;
    o)  ORDINAL=$OPTARG
        ;;
    d)  DOWNSAMPLE="-ds"
    esac
done

cd "$(dirname "$0")"

# if downsample arg is present choose the downsample config. Otherwise regular server config
CONFIG=conf/timeseries-filodb-server$DOWNSAMPLE.conf

if [ $ORDINAL -eq "0" ]; then
  ADDL_JAVA_OPTS=" -Dfilodb.cluster-discovery.localhost-ordinal=0"
elif [ $ORDINAL -eq "1" ]; then
  ADDL_JAVA_OPTS=" -Dfilodb.cluster-discovery.localhost-ordinal=1 -Dakka.remote.netty.tcp.port=3552 -Dfilodb.http.bind-port=0 -Dkamon.environment.service=filodb-local2"
else
  echo "Only ordinals 0 and 1 are supported"
  exit
fi

if [ ! -f standalone/target/scala-2.13/standalone-assembly-*-SNAPSHOT.jar ]; then
    echo "Standalone assembly not found. Building..."
    sbt standalone/assembly
fi

# JDK 17+ module system opens required for Kryo serialization of internal Java classes
JDK17_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED"

FIXED_JAVA_OPTS="-Xmx2G $JDK17_OPTS -Dconfig.file=$CONFIG -Dlogback.configurationFile=conf/logback-dev.xml "

echo "Starting FiloDB standalone server ..."
echo "Java Opts Used: $FIXED_JAVA_OPTS $ADDL_JAVA_OPTS"
java $FIXED_JAVA_OPTS $ADDL_JAVA_OPTS -cp standalone/target/scala-2.13/standalone-assembly-*-SNAPSHOT.jar filodb.standalone.FiloServer
