package it.uniroma2.ing.dicii.sabd.flink.query3;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class Query3RankWindow extends ProcessAllWindowFunction<String, String, TimeWindow> {


    @Override
    public void process(Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
        String result = iterable.iterator().next();
        collector.collect(new Date(context.window().getStart()) + result);
    }
}
