#!/bin/bash

sbt -Drust.optimize=true "jmh/jmh:run -rf json -i 2 -wi 2 -f 1 \
 -jvmArgsAppend -Dlogback.configurationFile=../conf/logback-perf.xml
 -jvmArgsAppend -XX:MaxInlineLevel=20 \
 -jvmArgsAppend -Xmx4g \
 -jvmArgsAppend -XX:MaxInlineSize=99 \
 -jvmArgsAppend -Dkamon.enabled=false \
 filodb.jmh.HistVectorBenchmark \
 filodb.jmh.Base2ExponentialHistogramQueryBenchmark \
 filodb.jmh.QueryHiCardInMemoryBenchmark \
 filodb.jmh.QueryInMemoryBenchmark \
 filodb.jmh.QueryAndIngestBenchmark \
 filodb.jmh.IngestionBenchmark \
 filodb.jmh.QueryOnDemandBenchmark \
 filodb.jmh.GatewayBenchmark \
 filodb.jmh.PartKeyLuceneIndexBenchmark \
 filodb.jmh.PartKeyTantivyIndexBenchmark"

# Add below argument to enable profiling
# -prof \"async:libPath=/path/to/async-profiler-3.0-macos/lib/libasyncProfiler.dylib;event=cpu;output=flamegraph;dir=./profile-results\" \
