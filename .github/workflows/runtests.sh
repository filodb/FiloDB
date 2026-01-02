#!/bin/sh
set -e

echo "Setting up Cassandra..."
wget -q -O - https://archive.apache.org/dist/cassandra/KEYS | sudo apt-key add -
sudo sh -c 'echo "deb http://archive.apache.org/dist/cassandra/debian 40x main" > /etc/apt/sources.list.d/cassandra.list'
sudo apt update
sudo apt install cassandra

echo "Verifying Java 21 and Scala 2.13 versions..."
java -version
echo "Expected: Java 21.x.x"

echo "SBT project Scala version:"
sbt "show scalaVersion" || echo "Could not determine Scala version"
echo "Expected: Scala 2.13.12"

echo "Starting compilation test with JDK 21..."
sbt compile || {
    echo "Compilation failed with JDK 21!"
    exit 1
}

echo "Compilation successful with JDK 21! Running tests with coverage..."
set +e
sbt coverage test coverageAggregate
test_result=$?

if [ $test_result -eq 0 ]; then
    echo "All tests passed successfully with JDK 21!"
else
    echo "Some tests failed, but continuing to generate coverage report..."
fi

exit $test_result
