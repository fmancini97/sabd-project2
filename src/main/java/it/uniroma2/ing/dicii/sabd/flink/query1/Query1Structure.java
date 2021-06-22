package it.uniroma2.ing.dicii.sabd.flink.query1;

import it.uniroma2.ing.dicii.sabd.TripData;
import it.uniroma2.ing.dicii.sabd.Utils.KafkaProperties;
import it.uniroma2.ing.dicii.sabd.Utils.MonthlyWindowAssigner;
import it.uniroma2.ing.dicii.sabd.Utils.TimeIntervalEnum;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;


import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;

public class Query1Structure {

    final static double channelOfSicilyLon = 11.797697;
    public static void build(DataStream<Tuple2<Long,String>> source, TimeIntervalEnum timeIntervalEnum) throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        Constructor<? extends TumblingEventTimeWindows> timeIntervalConstructor = null;

        timeIntervalConstructor = timeIntervalEnum.getTimeIntervalClass().getConstructor();


        DataStream<TripData> stream = source.map((MapFunction<Tuple2<Long, String>, TripData>) tuple -> {
            String[] info = tuple.f1.split(",");
            return new TripData(info[10],info[0],Double.parseDouble(info[3]),
                    Double.parseDouble(info[4]), tuple.f0, Integer.parseInt(info[1]));
        }).filter((FilterFunction<TripData>) tripData -> tripData.getLon() < channelOfSicilyLon)
        .name("stream-query1");

        Properties props = KafkaProperties.getFlinkProducerProperties();

        stream.keyBy(TripData::getCell)
                .window(timeIntervalConstructor.newInstance())
                .aggregate(new Query1Aggregator(), new Query1Window())
                .name("query1" + timeIntervalEnum.getTimeIntervalName())
                .map((MapFunction<Query1Outcome, String>) query1Outcome -> {
                    return query1OutcomeToResultMap(timeIntervalEnum,query1Outcome);
                })
                .addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY1_TOPIC + timeIntervalEnum.getTimeIntervalName(),
                        new FlinkOutputSerializer(KafkaProperties.QUERY1_TOPIC + timeIntervalEnum.getTimeIntervalName()),
                        props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query1" + timeIntervalEnum.getTimeIntervalName() + "Sink");


    }

    private static String query1OutcomeToResultMap(TimeIntervalEnum timeIntervalEnum, Query1Outcome query1Outcome) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        StringBuilder builder = new StringBuilder();
        Date date = query1Outcome.getDate();
        builder.append(simpleDateFormat.format(date))
                .append(",").append(query1Outcome.getCellId());

        int daysOfActualTimeInterval = timeIntervalEnum.getNumDays(date);
        System.out.println(daysOfActualTimeInterval);
        //todo stampare tutte le tipologie di navi, anche quelle con v=0
        query1Outcome.getTypeMap().forEach((k,v) -> {
            builder.append(",").append(k).append(",").append(String.format(Locale.ENGLISH, "%.2g",(double)v/daysOfActualTimeInterval));
        });


        return builder.toString();
    }

}
