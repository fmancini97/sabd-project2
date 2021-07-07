package it.uniroma2.ing.dicii.sabd.kafkastreams.query1;

import it.uniroma2.ing.dicii.sabd.TripData;
import it.uniroma2.ing.dicii.sabd.flink.query1.Query1Accumulator;  //todo usa la stessa classe di flink, è da spostare?
import org.apache.kafka.streams.kstream.Aggregator;

public class Query1Aggregator implements Aggregator<String, TripData, Query1Accumulator> {

    @Override
    public Query1Accumulator apply(String s, TripData data, Query1Accumulator query1Accumulator) {
        query1Accumulator.add(data.getShipType(), data.getTripId());
        return query1Accumulator;
    }
}
