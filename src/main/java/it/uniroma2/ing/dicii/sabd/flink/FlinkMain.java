package it.uniroma2.ing.dicii.sabd.flink;

import it.uniroma2.ing.dicii.sabd.flink.query3.Query3Structure;
import it.uniroma2.ing.dicii.sabd.utils.KafkaProperties;
import it.uniroma2.ing.dicii.sabd.utils.TimeIntervalEnum;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Properties;


public class FlinkMain {


    static SimpleDateFormat[] dateFormats = {new SimpleDateFormat("dd/MM/yy HH:mm"),
            new SimpleDateFormat("dd-MM-yy HH:mm")};


    public static void main(String[] args) {

        //setup flink environment
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();


        Properties props = KafkaProperties.getFlinkConsumerProperties();
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(KafkaProperties.SOURCE_TOPIC, new SimpleStringSchema(), props);

        consumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1)));

        DataStream<Tuple2<Long, String>> stream = environment
                .addSource(consumer)
                .map(new MapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(String s) throws Exception {
                            String[] values = s.split(",");
                            String dateString = values[7];
                            Long timestamp = null;
                            for (SimpleDateFormat dateFormat : dateFormats) {
                                try {
                                    timestamp = dateFormat.parse(dateString).getTime();
                                    break;
                                } catch (ParseException ignored) {
                                }
                            }

                            if (timestamp == null) {
                                System.out.println("Timestamp null!");
                                //ignore tuple
                            }

                            return new Tuple2<>(timestamp, s);
                        }
                    })
                .name("stream-source");


        /*
        for(TimeIntervalEnum timeIntervalEnum: TimeIntervalEnum.values()){
            try {
                //Query1Structure.build(stream, timeIntervalEnum);
                //Query2Structure.build(stream,timeIntervalEnum);
            } catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
                e.printStackTrace();
            }
        }*/

        try {
            Query3Structure.build(stream, TimeIntervalEnum.WEEKLY);
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }


        try {
            environment.execute("SABD Project 2");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}