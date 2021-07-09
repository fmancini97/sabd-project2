package it.uniroma2.ing.dicii.sabd.flink.query2;

import it.uniroma2.ing.dicii.sabd.data.TripData;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * It aggregate cell attendance values for a single cell
 */
public class Query2IntermediateAggregator implements AggregateFunction<TripData, Query2IntermediateAccumulator, Query2IntermediateOutcome> {


    @Override
    public Query2IntermediateAccumulator createAccumulator() {
        return new Query2IntermediateAccumulator();
    }

    @Override
    public Query2IntermediateAccumulator add(TripData tripData, Query2IntermediateAccumulator query2IntermediateAccumulator) {
        query2IntermediateAccumulator.add(tripData.getTripId());
        return query2IntermediateAccumulator;
    }

    @Override
    public Query2IntermediateAccumulator merge(Query2IntermediateAccumulator acc1, Query2IntermediateAccumulator acc2) {
        acc2.getAttendances().forEach(acc1::add);
        return acc1;
    }

    @Override
    public Query2IntermediateOutcome getResult(Query2IntermediateAccumulator acc) {
        return new Query2IntermediateOutcome(acc.getAttendances());
    }


}
