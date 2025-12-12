FROM docker.apple.com/telemetry/applejdk11:latest

COPY standalone/target/scala-2.12/standalone-assembly-0.9-SNAPSHOT.jar /app/
COPY conf /app/conf
COPY ./filodb-dev-start-docker.sh /app

ENTRYPOINT "./filodb-dev-start-docker.sh"