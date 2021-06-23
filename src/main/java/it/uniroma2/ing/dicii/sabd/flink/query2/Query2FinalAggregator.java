package it.uniroma2.ing.dicii.sabd.flink.query2;

import org.apache.flink.api.common.functions.AggregateFunction;

public class Query2FinalAggregator implements AggregateFunction<Query2IntermediateOutcome, Query2Outcome, Query2Outcome> {
    @Override
    public Query2Outcome createAccumulator() {
        return new Query2Outcome();
    }

    @Override
    public Query2Outcome add(Query2IntermediateOutcome query2IntermediateOutcome, Query2Outcome query2Outcome) {
        query2Outcome.add(query2IntermediateOutcome);
        return query2Outcome;
    }

    @Override
    public Query2Outcome merge(Query2Outcome acc1, Query2Outcome acc2) {
        acc2.getAmTop3().forEach(acc1::addAM);
        acc2.getPmTop3().forEach(acc1::addPM);
        return acc1;
    }

    @Override
    public Query2Outcome getResult(Query2Outcome query2Outcome) {
        return query2Outcome;
    }


}
