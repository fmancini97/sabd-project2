#!/usr/bin/env sh
dir="$( dirname "$0" )"

docker-compose -f "$dir"/docker-compose.yml exec jobmanager flink run --detached -c it.uniroma2.ing.dicii.sabd.flink.FlinkMain /target/sabd-project2-1.0-SNAPSHOT-jar-with-dependencies.jar
