package it.uniroma2.ing.dicii.sabd.flink;

import it.uniroma2.ing.dicii.sabd.data.TripData;
import it.uniroma2.ing.dicii.sabd.flink.query1.Query1Structure;
import it.uniroma2.ing.dicii.sabd.flink.query2.Query2Structure;
import it.uniroma2.ing.dicii.sabd.flink.query3.Query3Structure;
import it.uniroma2.ing.dicii.sabd.kafka.KafkaProperties;
import it.uniroma2.ing.dicii.sabd.utils.timeIntervals.TimeIntervalEnum;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;


import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * It builds Flink topology
 */
public class FlinkMain {


    static SimpleDateFormat[] dateFormats = {new SimpleDateFormat("dd/MM/yy HH:mm"),
            new SimpleDateFormat("dd-MM-yy HH:mm")};


    public static void main(String[] args) {
        Logger log = Logger.getLogger(FlinkMain.class.getSimpleName());

        //setup flink environment
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();


        Properties props = KafkaProperties.getFlinkConsumerProperties();
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(KafkaProperties.SOURCE_TOPIC, new SimpleStringSchema(), props);

        consumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1)));

        DataStream<TripData> stream = environment
                .addSource(consumer)
                .name("source")
                .flatMap(new FlatMapFunction<String, TripData>() {
                    @Override
                    public void flatMap(String s, Collector<TripData> collector) throws Exception {

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

                            if(timestamp == null)
                                return;

                            TripData data = new TripData(values[10],values[0],Double.parseDouble(values[3]),
                                    Double.parseDouble(values[4]), timestamp, Integer.parseInt(values[1]), timestamp);
                            if(data.isValid())
                                collector.collect(data);

                        }
                }).name("parseData");

        try {
            for (TimeIntervalEnum timeIntervalEnum: Arrays.asList(TimeIntervalEnum.WEEKLY, TimeIntervalEnum.MONTHLY)) {
                Query1Structure.build(stream, timeIntervalEnum);
                Query2Structure.build(stream,timeIntervalEnum);
            }

            for (Tuple2<String, Time> timeTuple2: Arrays.asList(new Tuple2<>("Hourly", Time.hours(1)), new Tuple2<>("EveryTwoHours", Time.hours(2)))) {
                Query3Structure.build(stream, timeTuple2.f0, timeTuple2.f1);
            }

        } catch (InvocationTargetException | InstantiationException |  IllegalAccessException | NoSuchMethodException e) {
            log.log(Level.SEVERE, "Error while creating execution environment: {0}", e.getMessage());
        }


        try {
            environment.execute("SABD Project 2");
        } catch (Exception e) {
            e.printStackTrace();
            log.log(Level.SEVERE, "Error while executing environment: {0}", e.getMessage());
        }

    }
}