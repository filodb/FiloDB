#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

if [ ! -f standalone/target/scala-2.11/standalone-assembly-*.telemetry-SNAPSHOT.jar ]; then
    echo "Standalone assembly not found. Building..."
    sbt standalone/assembly
fi

echo "Starting FiloDB standalone server..."
java -Xmx4G -Dconfig.file=conf/timeseries-filodb-server.conf -cp standalone/target/scala-2.11/standalone-assembly-*.telemetry-SNAPSHOT.jar filodb.standalone.FiloServer  &

echo "Waiting 20s so server can come up ..."
sleep 20

echo "Configuring the timeseries dataset..."
./filo-cli '-Dakka.remote.netty.tcp.hostname=127.0.0.1' --host 127.0.0.1 --dataset timeseries --command setup --filename conf/timeseries-dev-source.conf

echo "Done."
