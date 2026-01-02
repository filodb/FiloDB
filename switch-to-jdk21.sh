#!/bin/bash
# Script to switch to JDK 21 for this terminal session
# Usage: source switch-to-jdk21.sh

# Try to find JDK 21 using java_home utility (macOS)
if command -v /usr/libexec/java_home &> /dev/null; then
    JDK21_HOME=$(/usr/libexec/java_home -v 21 2>/dev/null)
fi

# Fallback to common paths if java_home didn't find it
if [ -z "$JDK21_HOME" ] || [ ! -d "$JDK21_HOME" ]; then
    # Try common installation paths
    for path in \
        "/Library/Java/JavaVirtualMachines/applejdk-21"*"/Contents/Home" \
        "/Library/Java/JavaVirtualMachines/jdk-21"*"/Contents/Home" \
        "/Library/Java/JavaVirtualMachines/temurin-21"*"/Contents/Home" \
        "/Library/Java/JavaVirtualMachines/zulu-21"*"/Contents/Home" \
        "/usr/lib/jvm/java-21-openjdk"* \
        "/usr/lib/jvm/jdk-21"*
    do
        # Use first match that exists
        for match in $path; do
            if [ -d "$match" ]; then
                JDK21_HOME="$match"
                break 2
            fi
        done
    done
fi

if [ -d "$JDK21_HOME" ]; then
    export JAVA_HOME="$JDK21_HOME"
    export PATH="$JAVA_HOME/bin:$PATH"
    echo "Switched to JDK 21:"
    java -version
else
    echo "JDK 21 not found on this system"
    echo ""
    echo "Please install JDK 21 first. You can:"
    echo "1. Download from: https://adoptium.net/temurin/releases/?version=21"
    echo "2. Or use Homebrew: brew install openjdk@21"
    echo ""
    echo "Available JDKs on your system:"
    /usr/libexec/java_home -V 2>&1 || ls /usr/lib/jvm/ 2>/dev/null || echo "Could not list available JDKs"
fi
