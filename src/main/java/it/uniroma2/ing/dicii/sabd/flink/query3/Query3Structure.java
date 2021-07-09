package it.uniroma2.ing.dicii.sabd.flink.query3;

import it.uniroma2.ing.dicii.sabd.data.TripData;
import it.uniroma2.ing.dicii.sabd.flink.query1.FlinkOutputSerializer;
import it.uniroma2.ing.dicii.sabd.kafka.KafkaProperties;
import it.uniroma2.ing.dicii.sabd.utils.timeIntervals.TimeIntervalEnum;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import scala.Int;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Properties;

/**
 * It implements query3 logic
 */
public class Query3Structure {

    public static void build(DataStream<TripData> stream, String durationName, Time duration) throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {

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

                        long endTimestamp = 0;
                        try {
                            endTimestamp = endDateFormat.parse(endDateString).getTime();
                        } catch (ParseException e) {
                           e.printStackTrace();
                        }

                        long actualTime = tripData.getTimestamp();

                        return Math.max(endTimestamp - actualTime + tenMinutes, tenMinutes);
                    }
                }))
                .trigger(SessionWindowTrigger.create())
                .aggregate(new Query3Aggregator(), new Query3Window())
                .name("query3" + durationName + "-computeDistance")
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Double>>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.f1))
                .windowAll(TumblingEventTimeWindows.of(duration))
                .allowedLateness(Time.minutes(30))
                .aggregate(new Query3Ranking(), new Query3RankWindow())
                .name("query3" + durationName + "-rank");

        resultStream.addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY3_TOPIC + durationName,
                new FlinkOutputSerializer(KafkaProperties.QUERY3_TOPIC + durationName),
                props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query3" + durationName + "-sink").setParallelism(1);

    }

}
