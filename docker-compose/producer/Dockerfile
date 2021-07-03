FROM openjdk:11-slim

ADD ./data /data/
ADD ./target/ /target/

ENV TIME_RANGE=5

CMD java -cp /target/sabd-project2-1.0-SNAPSHOT-jar-with-dependencies.jar it.uniroma2.ing.dicii.sabd.Injector $TIME_RANGE true $FLINK_URL