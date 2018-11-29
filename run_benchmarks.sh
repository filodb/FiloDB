#!/bin/bash
sbt "jmh/jmh:run -rf json -i 15 -wi 10 -f3 -jvmArgsAppend -XX:MaxInlineLevel=20 \
 -jvmArgsAppend -Xmx4g -jvmArgsAppend -XX:MaxInlineSize=99 \
 -prof jmh.extras.JFR:dir=/tmp/filo-jmh
 filodb.jmh.QueryInMemoryBenchmark \
 filodb.jmh.QueryAndIngestBenchmark \
 filodb.jmh.IngestionBenchmark \
 filodb.jmh.QueryOnDemandBenchmark \
 filodb.jmh.GatewayBenchmark \
 filodb.jmh.PartKeyIndexBenchmark"
