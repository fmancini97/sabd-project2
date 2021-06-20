package it.uniroma2.ing.dicii.sabd.flink.query1;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.Date;

public class Query1Window extends ProcessAllWindowFunction<Query1Outcome, Query1Outcome, TimeWindow> {
    @Override
    public void process(Context context, Iterable<Query1Outcome> iterable, Collector<Query1Outcome> collector) {
        iterable.forEach(elem -> {
            elem.setDate(new Date(context.window().getStart()));
            collector.collect(elem);
        });
    }
}
