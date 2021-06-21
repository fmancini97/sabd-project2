package it.uniroma2.ing.dicii.sabd.flink.query1;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;


public class Query1Window
        extends ProcessWindowFunction<Query1Outcome, Query1Outcome, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<Query1Outcome> iterable, Collector<Query1Outcome> collector) throws Exception {
        Query1Outcome query1Outcome = iterable.iterator().next();
        query1Outcome.setDate(new Date(context.window().getStart()));
        query1Outcome.setCellId(key);
        collector.collect(query1Outcome);
    }
}
