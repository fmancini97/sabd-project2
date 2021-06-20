package it.uniroma2.ing.dicii.sabd.flink;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;


import it.uniroma2.ing.dicii.sabd.Utils.KafkaProperties;
import it.uniroma2.ing.dicii.sabd.flink.query1.Query1Structure;
import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;


public class FlinkMain {

    static SimpleDateFormat[] dateFormats = {new SimpleDateFormat("dd/MM/yy HH:mm"),
            new SimpleDateFormat("dd-MM-yy HH:mm")};

    public static void main(String[] args){

        //setup flink environment
        Configuration conf = new Configuration();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);


        Properties props = KafkaProperties.getFlinkConsumerProperties();


        DataStream<Tuple2<Long, String>> stream = environment
                .addSource(new FlinkKafkaConsumer<>(KafkaProperties.SOURCE_TOPIC, new SimpleStringSchema(), props))
                .flatMap(new FlatMapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<Long, String>> collector) {

                        String[] values = s.split(",");
                        String dateString = values[7];
                        Long timestamp = null;
                        for (SimpleDateFormat dateFormat: dateFormats) {
                            try {
                                timestamp = dateFormat.parse(dateString).getTime();
                                break;
                            } catch (ParseException ignored) { }
                        }

                        if (timestamp == null) {
                            System.out.println("Timestamp null!");
                            //ignore tuple
                        }

                        collector.collect(new Tuple2<>(timestamp, s));

                    }
                }).assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple2<Long, String>>() {
                    @Override
                    public WatermarkGenerator<Tuple2<Long, String>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new AscendingTimestampsWatermarks<>();
                    }
                })
                .name("stream-source");

        Query1Structure.build(stream);

        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

/*
 .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, String>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<Long, String> tuple) {
                        return tuple.f0;
                    }
                })
* */