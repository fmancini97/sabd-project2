#!/usr/bin/env sh

dir="$( dirname "$0" )"

# Compiling the program
mvn -f "$dir"/pom.xml clean compile assembly:single -Pcluster

# Creating Consumer and Producer docker images
mkdir "$dir"/docker-compose/producer/target/ "$dir"/docker-compose/consumer/target/ "$dir"/docker-compose/consumer/output/
cp "$dir"/target/sabd-project2-1.0-SNAPSHOT-jar-with-dependencies.jar "$dir"/docker-compose/producer/target/
cp "$dir"/target/sabd-project2-1.0-SNAPSHOT-jar-with-dependencies.jar "$dir"/docker-compose/consumer/target/
chmod -R go+rwx "$dir"/docker-compose/consumer/output/
docker-compose --project-directory "$dir"/docker-compose build
