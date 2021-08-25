#!/bin/sh
wget -q -O - https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -
sudo sh -c 'echo "deb http://www.apache.org/dist/cassandra/debian 40x main" > /etc/apt/sources.list.d/cassandra.list'
sudo apt update
sudo apt install cassandra
sbt test
