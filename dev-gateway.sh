#!/usr/bin/env bash
java -cp gateway/target/scala-2.11/gateway-* filodb.gateway.GatewayServer conf/timeseries-dev-source.conf &