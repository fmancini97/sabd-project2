package it.uniroma2.ing.dicii.sabd.flink.query3;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Query3RankWindow extends ProcessAllWindowFunction<String, String, TimeWindow> {

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    @Override
    public void process(Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
        String result = iterable.iterator().next();
        String dateString = simpleDateFormat.format(new Date(context.window().getStart()));
        collector.collect(dateString + result);
    }
}
