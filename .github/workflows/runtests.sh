#!/bin/sh
set -e
wget -q -O - https://archive.apache.org/dist/cassandra/KEYS | sudo apt-key add -
sudo sh -c 'echo "deb http://archive.apache.org/dist/cassandra/debian 40x main" > /etc/apt/sources.list.d/cassandra.list'
sudo apt update
sudo apt install cassandra
set +e
sbt coverage test coverageAggregate