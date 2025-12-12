#!/bin/bash
# Script to switch to JDK 17 for this terminal session
export JAVA_HOME=/Library/Java/JavaVirtualMachines/applejdk-17.0.10.7.1.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH

echo "Switched to JDK 17:"
java -version
