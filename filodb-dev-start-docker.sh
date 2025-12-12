#!/usr/bin/env bash
set -e
#set -x

function showHelp {
        echo "`basename $0` [-h] [-c arg] [-l arg] [-p]"
        echo "   -h help"
        echo "   -c takes server config path as argument"
        echo "   -l takes log file suffix as argument"
        echo "   -p selects a randomly available akka tcp and http port"
}

CONFIG=conf/timeseries-filodb-server-docker.conf
LOG_SUFFIX=1
AKKA_PORT_ARG=""

while getopts "hc:l:p" opt; do
    case "$opt" in
    h|\?) showHelp
        exit 1
        ;;
    c)  CONFIG=$OPTARG
        ;;
    l)  LOG_SUFFIX=$OPTARG
        ;;
    p)  PORTS_ARG="-Dakka.remote.netty.tcp.port=0 -Dfilodb.http.bind-port=0 -Dkamon.prometheus.embedded-server.port=9096"
        ;;
    esac
done

cd "$(dirname "$0")"

#if [ ! -f standalone/target/scala-2.12/standalone-assembly-*-SNAPSHOT.jar ]; then
#    echo "Standalone assembly not found. Building..."
#    sbt standalone/assembly
#fi

# JDK 17+ module system opens required for Kryo serialization of internal Java classes
JDK17_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"

echo "Starting FiloDB standalone server..."
java -Xmx4G $JDK17_OPTS $PORTS_ARG -Dconfig.file=$CONFIG -DlogSuffix=$LOG_SUFFIX -cp standalone-assembly-*-SNAPSHOT.jar filodb.standalone.FiloServer
sleep 1000000
