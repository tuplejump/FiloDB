#!/usr/bin/env bash
#
# Starts a local Gateway for ingesting data into FiloDB (run with no options)
# Type --help to see options - options include generating random test data and exiting.
args=${@:-"conf/timeseries-dev-source.conf"}
java -Dconfig.file=conf/timeseries-filodb-server.conf  \
     -Dkamon.prometheus.embedded-server.port=9097  \
     -cp gateway/target/scala-2.12/gateway-* filodb.gateway.GatewayServer $args &
