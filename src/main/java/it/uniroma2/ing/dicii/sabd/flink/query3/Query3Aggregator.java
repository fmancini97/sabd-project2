package it.uniroma2.ing.dicii.sabd.flink.query3;

import it.uniroma2.ing.dicii.sabd.TripData;
import org.apache.flink.api.common.functions.AggregateFunction;

public class Query3Aggregator implements AggregateFunction<TripData, Query3Accumulator, Double> {


    @Override
    public Query3Accumulator createAccumulator() {
        return new Query3Accumulator();
    }

    @Override
    public Query3Accumulator add(TripData tripData, Query3Accumulator query3Accumulator) {
        query3Accumulator.add(tripData);
        return query3Accumulator;
    }

    @Override
    public Double getResult(Query3Accumulator query3Accumulator) {
        return query3Accumulator.getDistance();
    }

    @Override
    public Query3Accumulator merge(Query3Accumulator acc1, Query3Accumulator acc2) {
        System.out.println("ERRRROOOOREEEEEEE");
        return null;
    }
}
