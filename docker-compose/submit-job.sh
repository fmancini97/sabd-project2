#!/usr/bin/env bash
dir="$( dirname "$0" )"

PARALLELISM=1

if [[ $1 =~ ^-?[0-9]+$ ]]; then
   PARALLELISM=$1
fi
echo "Submitting job with parallelism equal to $PARALLELISM"
docker-compose -f "$dir"/docker-compose.yml exec jobmanager flink run -p "$PARALLELISM" --detached -c it.uniroma2.ing.dicii.sabd.flink.FlinkMain /target/sabd-project2-1.0-SNAPSHOT-jar-with-dependencies.jar
