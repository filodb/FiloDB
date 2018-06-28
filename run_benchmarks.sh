#!/bin/bash
sbt "jmh/jmh:run -i 15 -wi 10 -f3 -jvmArgsAppend -XX:MaxInlineLevel=20 \
 -jvmArgsAppend -Xmx4g -jvmArgsAppend -XX:MaxInlineSize=99 \
 filodb.jmh.QueryInMemoryBenchmark"

sbt "jmh/jmh:run -i 15 -wi 10 -f3 -jvmArgsAppend -XX:MaxInlineLevel=20 \
 -jvmArgsAppend -Xmx4g -jvmArgsAppend -XX:MaxInlineSize=99 \
 filodb.jmh.IngestionBenchmark"