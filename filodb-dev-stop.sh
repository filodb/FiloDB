#!/usr/bin/env bash
set -e
# This will stop all running filodb servers
pid=$(ps -ef | grep filodb.standalone.FiloServer | grep -v grep | awk '{print $2}')
kill $pid