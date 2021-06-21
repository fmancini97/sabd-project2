package it.uniroma2.ing.dicii.sabd.flink.query1;

import it.uniroma2.ing.dicii.sabd.TripData;
import it.uniroma2.ing.dicii.sabd.Utils.KafkaProperties;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class Query1Structure {

    public static void build(DataStream<Tuple2<Long,String>> source){


        DataStream<TripData> stream = source.flatMap(new FlatMapFunction<Tuple2<Long, String>, TripData>() {
            @Override
            public void flatMap(Tuple2<Long, String> tuple, Collector<TripData> collector) throws Exception {
                String[] info = tuple.f1.split(",");
                TripData data = new TripData(info[10],info[0],Double.parseDouble(info[3]),
                        Double.parseDouble(info[4]), tuple.f0, Integer.parseInt(info[1]));
                System.out.println(new Date(data.getTimestamp()));
                collector.collect(data);
            }
        }).name("stream-query1");

        Properties props = KafkaProperties.getFlinkProducerProperties();
        stream.keyBy(TripData::getCell)
                .window(TumblingEventTimeWindows.of(Time.days(7), Time.days(-3)))
                .aggregate(new Query1Aggregator(), new Query1Window())
                .name("query1Weekly")
                .map((MapFunction<Query1Outcome, String>) query1Outcome -> {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

                    StringBuilder builder = new StringBuilder();

                    builder.append(simpleDateFormat.format(query1Outcome.getDate()))
                            .append(",").append(query1Outcome.getCellId());

                    query1Outcome.getTypeMap().forEach((k,v) -> {
                        builder.append(",").append(k).append(",").append(String.format("%.2g",(double)v/7));
                    });
                    return builder.toString();
                })
                .addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY1_WEEKLY_TOPIC,
                new FlinkOutputSerializer(KafkaProperties.QUERY1_WEEKLY_TOPIC),
                props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query1WeeklySink");
    }
}
