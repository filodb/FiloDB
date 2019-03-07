#!/usr/bin/env bash
java -Dconfig.file=conf/timeseries-filodb-server.conf  \
     -cp gateway/target/scala-2.11/gateway-* filodb.gateway.GatewayServer conf/timeseries-dev-source.conf &