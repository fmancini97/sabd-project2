package it.uniroma2.ing.dicii.sabd.flink.query3;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class Query3Ranking implements AggregateFunction<Tuple3<String, Long, Double>, Query3RankAccumulator, String> {


    @Override
    public Query3RankAccumulator createAccumulator() {
        return new Query3RankAccumulator();
    }

    @Override
    public Query3RankAccumulator add(Tuple3<String, Long, Double> stringDoubleTuple2, Query3RankAccumulator query3RankAccumulator) {
        query3RankAccumulator.add(new Tuple2<>(stringDoubleTuple2.f0, stringDoubleTuple2.f2));
        return query3RankAccumulator;
    }

    @Override
    public String getResult(Query3RankAccumulator query3RankAccumulator) {
        return query3RankAccumulator.getResult();
    }

    @Override
    public Query3RankAccumulator merge(Query3RankAccumulator query3RankAccumulator, Query3RankAccumulator acc1) {
        return null;
    }
}
