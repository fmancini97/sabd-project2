#!/usr/bin/env sh

mvn clean compile assembly:single
mkdir ./docker-compose/producer/target/
cp ./target/sabd-project2-1.0-SNAPSHOT-jar-with-dependencies.jar ./docker-compose/producer/target/