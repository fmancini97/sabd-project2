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

                            if (timestamp == null) {
                                System.out.println("Timestamp null!");
                                //todo ignore tuple
                            }

                            return new TripData(values[10],values[0],Double.parseDouble(values[3]),
                                    Double.parseDouble(values[4]), timestamp, Integer.parseInt(values[1]), timestamp);

                        }
                    })
                .name("stream-data");


        for(TimeIntervalEnum timeIntervalEnum: TimeIntervalEnum.values()){
            try {
                switch (timeIntervalEnum){
                    case HOURLY:
                    case EVERYTWOHOURS:
                        Query3Structure.build(stream,timeIntervalEnum);
                        break;
                    case WEEKLY:
                    case MONTHLY:
                        Query1Structure.build(stream, timeIntervalEnum);
                        Query2Structure.build(stream,timeIntervalEnum);
                        break;
                }
            } catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
                e.printStackTrace();
            }
        }


        try {
            environment.execute("SABD Project 2");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}