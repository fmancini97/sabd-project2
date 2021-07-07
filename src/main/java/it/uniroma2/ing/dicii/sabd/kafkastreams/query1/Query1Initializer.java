package it.uniroma2.ing.dicii.sabd.kafkastreams.query1;

import it.uniroma2.ing.dicii.sabd.flink.query1.Query1Accumulator;
import org.apache.kafka.streams.kstream.Initializer;

public class Query1Initializer implements Initializer<Query1Accumulator> {
    @Override
    public Query1Accumulator apply() {
        return new Query1Accumulator();
    }
}
