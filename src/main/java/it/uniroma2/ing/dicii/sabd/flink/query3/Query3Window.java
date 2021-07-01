package it.uniroma2.ing.dicii.sabd.flink.query3;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Query3Window extends ProcessWindowFunction<Tuple2<Long, Double> , Tuple3<String, Long, Double>, String, TimeWindow> {


    @Override
    public void process(String key, Context context, Iterable<Tuple2<Long, Double>> iterable, Collector<Tuple3<String, Long, Double>> collector) throws Exception {
        Tuple2<Long, Double> result = iterable.iterator().next();
        collector.collect(new Tuple3<>(key, result.f0, result.f1));
    }
}
