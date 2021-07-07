package it.uniroma2.ing.dicii.sabd.flink;

import it.uniroma2.ing.dicii.sabd.TripData;
import it.uniroma2.ing.dicii.sabd.flink.query1.Query1Structure;
import it.uniroma2.ing.dicii.sabd.flink.query2.Query2Structure;
import it.uniroma2.ing.dicii.sabd.flink.query3.Query3Structure;
import it.uniroma2.ing.dicii.sabd.utils.KafkaProperties;
import it.uniroma2.ing.dicii.sabd.utils.TimeIntervalEnum;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;


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
                .map(new MapFunction<String, TripData>() {
                    @Override
                    public TripData map(String s) throws Exception {
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

                            return new TripData(values[10],values[0],Double.parseDouble(values[3]),
                                    Double.parseDouble(values[4]), timestamp, Integer.parseInt(values[1]), timestamp);

                        }
                    })
                .name("stream-data");



        try {
            for (TimeIntervalEnum timeIntervalEnum: Arrays.asList(TimeIntervalEnum.WEEKLY, TimeIntervalEnum.MONTHLY)) {
                Query1Structure.build(stream, timeIntervalEnum);
                Query2Structure.build(stream,timeIntervalEnum);
            }

            for (TimeIntervalEnum timeIntervalEnum: Arrays.asList(TimeIntervalEnum.HOURLY, TimeIntervalEnum.EVERYTWOHOURS)) {
                Query3Structure.build(stream, timeIntervalEnum);
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