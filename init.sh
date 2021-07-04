#!/usr/bin/env sh

dir="$( dirname "$0" )"

mvn -f "$dir"/pom.xml clean compile assembly:single -Pcluster
mkdir "$dir"/docker-compose/producer/target/ "$dir"/docker-compose/consumer/target/
cp "$dir"/target/sabd-project2-1.0-SNAPSHOT-jar-with-dependencies.jar "$dir"/docker-compose/producer/target/
cp "$dir"/target/sabd-project2-1.0-SNAPSHOT-jar-with-dependencies.jar "$dir"/docker-compose/consumer/target/