package it.uniroma2.ing.dicii.sabd.flink.query2;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class Query2IntermediateWindow extends ProcessWindowFunction<Query2IntermediateOutcome, Query2IntermediateOutcome, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<Query2IntermediateOutcome> iterable, Collector<Query2IntermediateOutcome> collector) {
        Query2IntermediateOutcome query2IntermediateOutcome = iterable.iterator().next();
        query2IntermediateOutcome.setDate(new Date(context.window().getStart()));
        query2IntermediateOutcome.setCellId(key);
        collector.collect(query2IntermediateOutcome);
    }
}
