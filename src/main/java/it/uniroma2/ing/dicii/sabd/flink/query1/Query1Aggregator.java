package it.uniroma2.ing.dicii.sabd.flink.query1;

import it.uniroma2.ing.dicii.sabd.data.TripData;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * For each cell, it adds the tripid on a set based on the ship type
 */
public class Query1Aggregator implements AggregateFunction<TripData, Query1Accumulator, Query1Outcome> {

    @Override
    public Query1Accumulator createAccumulator() {
        return new Query1Accumulator();
    }

    @Override
    public Query1Accumulator add(TripData data, Query1Accumulator query1Accumulator) {
        query1Accumulator.add(data.getShipType(), data.getTripId());
        return query1Accumulator;
    }

    @Override
    public Query1Accumulator merge(Query1Accumulator acc1, Query1Accumulator acc2) {
        acc2.getTypeMap().forEach(acc1::add);
        return acc1;
    }

    @Override
    public Query1Outcome getResult(Query1Accumulator accumulator) {
        return new Query1Outcome(accumulator.getTypeMap());
    }

}
