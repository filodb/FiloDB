#!/bin/bash
sbt "jmh/jmh:run -rf json -i 7 -wi 3 -f3 -jvmArgsAppend -XX:MaxInlineLevel=20 \
 -jvmArgsAppend -Xmx4g -jvmArgsAppend -XX:MaxInlineSize=99 \
 filodb.jmh.PartKeyIndexBenchmark"
