package it.uniroma2.ing.dicii.sabd.flink.query3;

import it.uniroma2.ing.dicii.sabd.TripData;
import it.uniroma2.ing.dicii.sabd.flink.query1.FlinkOutputSerializer;
import it.uniroma2.ing.dicii.sabd.utils.KafkaProperties;
import it.uniroma2.ing.dicii.sabd.utils.MetricSink;
import it.uniroma2.ing.dicii.sabd.utils.MetricsGenerator;
import it.uniroma2.ing.dicii.sabd.utils.TimeIntervalEnum;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Properties;

public class Query3Structure {

    public static void build(DataStream<TripData> stream, TimeIntervalEnum timeIntervalEnum) throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {

        Properties props = KafkaProperties.getFlinkProducerProperties();

        SingleOutputStreamOperator<String> resultStream = stream.keyBy(TripData::getTripId)
                .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<TripData>() {
                    private final SimpleDateFormat endDateFormat = new SimpleDateFormat("dd-MM-yy HH:mm");
                    private static final long tenMinutes = 10 * 60 * 1000;

                    @Override
                    public long extract(TripData tripData) {
                        String tripID = tripData.getTripId();
                        String endDateString;
                        if (tripID.contains("_parking")) {
                            endDateString = tripID.substring(tripID.indexOf(" - ") + 3, tripID.indexOf("_parking"));
                        } else {
                            endDateString = tripID.substring(tripID.indexOf(" - ") + 3);
                        }
                        //todo cancellare le stampe che non servono effettivamente
                        if (!tripID.contains(" - ")) {
                            System.out.println("Errore");
                        }

                        if (endDateString.isEmpty()) {
                            System.out.println("String: " + endDateString);
                            System.out.println("TripID: " + tripID);
                        }
                        long endTimestamp = 0;
                        try {
                            endTimestamp = endDateFormat.parse(endDateString).getTime();
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.println("String: " + endDateString);
                            System.out.println("TripID: " + tripID);
                            System.exit(-1);
                        }

                        long actualTime = tripData.getTimestamp();

                        //System.out.println(Math.max(endTimestamp - actualTime + tenMinutes, tenMinutes));

                        return Math.max(endTimestamp - actualTime + tenMinutes, tenMinutes);
                    }



                }))
                .trigger(SessionWindowTrigger.create())
                .aggregate(new Query3Aggregator(), new Query3Window()).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Double>>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.f1).withIdleness(Duration.ofMillis(35)))
                .name("query2HalfDay")
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new Query3Ranking(), new Query3RankWindow());

        resultStream.addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY3_TOPIC + timeIntervalEnum.getTimeIntervalName(),
                new FlinkOutputSerializer(KafkaProperties.QUERY3_TOPIC + timeIntervalEnum.getTimeIntervalName()),
                props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query3" + timeIntervalEnum.getTimeIntervalName() + "Sink").setParallelism(1);

        //resultStream.addSink(new MetricSink()).setParallelism(1);

    }

}
