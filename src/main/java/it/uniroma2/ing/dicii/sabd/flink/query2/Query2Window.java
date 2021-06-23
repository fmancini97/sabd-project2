package it.uniroma2.ing.dicii.sabd.flink.query2;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class Query2Window extends ProcessWindowFunction<Query2Outcome, Query2Outcome, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<Query2Outcome> iterable, Collector<Query2Outcome> collector) {
        Query2Outcome query2Outcome = iterable.iterator().next();
        query2Outcome.setDate(new Date(context.window().getStart()));
        query2Outcome.setSeaType(key);
        collector.collect(query2Outcome);
    }
}
