package it.uniroma2.ing.dicii.sabd.flink.query3;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Query3Window extends ProcessWindowFunction<Double, Tuple2<String, Double>, String, TimeWindow> {


    @Override
    public void process(String key, Context context, Iterable<Double> iterable, Collector<Tuple2<String, Double>> collector) throws Exception {
        Double distance = iterable.iterator().next();
        collector.collect(new Tuple2<>(key, distance));
    }
}
