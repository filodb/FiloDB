#!/bin/bash
sbt -Drust.optimize=true "jmh/jmh:run -rf json -i 5 -wi 3 -f 1 -jvmArgsAppend -XX:MaxInlineLevel=20 \
 -jvmArgsAppend -Xmx4g -jvmArgsAppend -XX:MaxInlineSize=99 \
 filodb.jmh.QueryHiCardInMemoryBenchmark \
 filodb.jmh.QueryInMemoryBenchmark \
 filodb.jmh.QueryAndIngestBenchmark \
 filodb.jmh.IngestionBenchmark \
 filodb.jmh.QueryOnDemandBenchmark \
 filodb.jmh.GatewayBenchmark \
 filodb.jmh.PartKeyLuceneIndexBenchmark \
 filodb.jmh.PartKeyTantivyIndexBenchmark"
