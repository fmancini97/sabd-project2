package it.uniroma2.ing.dicii.sabd.flink.query2;

import it.uniroma2.ing.dicii.sabd.TripData;
import it.uniroma2.ing.dicii.sabd.utils.KafkaProperties;
import it.uniroma2.ing.dicii.sabd.utils.TimeIntervalEnum;
import it.uniroma2.ing.dicii.sabd.flink.query1.FlinkOutputSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Query2Structure {


    public static void build(DataStream<Tuple2<Long, String>> source, TimeIntervalEnum timeIntervalEnum) throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {

        Constructor<? extends TumblingEventTimeWindows> timeIntervalConstructor = null;

        timeIntervalConstructor = timeIntervalEnum.getTimeIntervalClass().getConstructor();


        DataStream<TripData> stream = source.map((MapFunction<Tuple2<Long, String>, TripData>) tuple -> {
            String[] info = tuple.f1.split(",");
            return new TripData(info[10],info[0],Double.parseDouble(info[3]),
                    Double.parseDouble(info[4]), tuple.f0, Integer.parseInt(info[1]), tuple.f0);
        }).name("stream-query2");

        Properties props = KafkaProperties.getFlinkProducerProperties();

        stream.keyBy(TripData::getCell)
                .window(TumblingEventTimeWindows.of(Time.hours(12)))
                .aggregate(new Query2IntermediateAggregator(), new Query2IntermediateWindow())
                .name("query2HalfDay")
                .keyBy(new KeySelector<Query2IntermediateOutcome, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> getKey(Query2IntermediateOutcome intermediateOutcome) throws Exception {
                        return new Tuple2<String, Integer>(intermediateOutcome.getCellId(), intermediateOutcome.getHourInDate());
                    }
                })
                .window(timeIntervalConstructor.newInstance())
                .reduce(new ReduceFunction<Query2IntermediateOutcome>() {
                    @Override
                    public Query2IntermediateOutcome reduce(Query2IntermediateOutcome t1, Query2IntermediateOutcome t2) throws Exception {
                        t1.setAttendance(t1.getAttendance()+t2.getAttendance());
                        return t1;
                    }
                })
                .keyBy(new KeySelector<Query2IntermediateOutcome, String>() {
                    @Override
                    public String getKey(Query2IntermediateOutcome query2IntermediateOutcome) throws Exception {
                        return query2IntermediateOutcome.getSeaType().toString();
                    }
                })
                .window(timeIntervalConstructor.newInstance())
                .aggregate(new Query2FinalAggregator(), new Query2Window())
                .map((MapFunction<Query2Outcome, String>) query2Outcome -> {
                    return query2OutcomeToResultMap(timeIntervalEnum,query2Outcome);
                })
                .addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY2_TOPIC + timeIntervalEnum.getTimeIntervalName(),
                        new FlinkOutputSerializer(KafkaProperties.QUERY2_TOPIC + timeIntervalEnum.getTimeIntervalName()),
                        props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query2" + timeIntervalEnum.getTimeIntervalName() + "Sink");

    }

    private static String query2OutcomeToResultMap(TimeIntervalEnum timeIntervalEnum, Query2Outcome query2Outcome) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        System.out.println("Kalimera");
        StringBuilder builder = new StringBuilder();
        Date date = query2Outcome.getDate();
        builder.append(simpleDateFormat.format(date))
                .append(",").append(query2Outcome.getSeaType());

        builder.append(",00:00-11:59");
        query2Outcome.getAmTop3().forEach(v -> {
            builder.append(",").append(v.getCellId());
        });

        builder.append(",12:00-23:59");
        query2Outcome.getPmTop3().forEach(v -> {
            builder.append(",").append(v.getCellId());
        });


        return builder.toString();
    }
}
