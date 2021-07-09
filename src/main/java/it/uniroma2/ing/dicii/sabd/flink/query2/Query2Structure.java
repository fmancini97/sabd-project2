package it.uniroma2.ing.dicii.sabd.flink.query2;

import it.uniroma2.ing.dicii.sabd.data.TripData;
import it.uniroma2.ing.dicii.sabd.kafka.KafkaProperties;
import it.uniroma2.ing.dicii.sabd.utils.timeIntervals.TimeIntervalEnum;
import it.uniroma2.ing.dicii.sabd.flink.query1.FlinkOutputSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * It implements query2 logic
 */
public class Query2Structure {


    public static void build(DataStream<TripData> stream, TimeIntervalEnum timeIntervalEnum) throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {

        Constructor<? extends TumblingEventTimeWindows> timeIntervalConstructor = null;

        timeIntervalConstructor = timeIntervalEnum.getTimeIntervalClass().getConstructor();


        Properties props = KafkaProperties.getFlinkProducerProperties();

        SingleOutputStreamOperator<String> resultStream = stream.keyBy(TripData::getCell)
                .window(TumblingEventTimeWindows.of(Time.hours(12)))
                .aggregate(new Query2IntermediateAggregator(), new Query2IntermediateWindow())
                .name("query2" + timeIntervalEnum.getTimeIntervalName() + "-dailyAttendance")
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
                }).name("query2" + timeIntervalEnum.getTimeIntervalName() + "-aggregatedAttendance")
                .keyBy(new KeySelector<Query2IntermediateOutcome, String>() {
                    @Override
                    public String getKey(Query2IntermediateOutcome query2IntermediateOutcome) throws Exception {
                        return query2IntermediateOutcome.getSeaType().toString();
                    }
                })
                .window(timeIntervalConstructor.newInstance())
                .aggregate(new Query2FinalAggregator(), new Query2Window())
                .name("query2" + timeIntervalEnum.getTimeIntervalName() + "-ranking")
                .map((MapFunction<Query2Outcome, String>) query2Outcome -> {
                    return query2OutcomeToResultMap(timeIntervalEnum,query2Outcome);
                }).name("query2" + timeIntervalEnum.getTimeIntervalName() + "-outputFormatMapper");

        resultStream.addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY2_TOPIC + timeIntervalEnum.getTimeIntervalName(),
                        new FlinkOutputSerializer(KafkaProperties.QUERY2_TOPIC + timeIntervalEnum.getTimeIntervalName()),
                        props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query2" + timeIntervalEnum.getTimeIntervalName() + "-sink");
    }

    private static String query2OutcomeToResultMap(TimeIntervalEnum timeIntervalEnum, Query2Outcome query2Outcome) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        StringBuilder builder = new StringBuilder();
        Date date = query2Outcome.getDate();
        builder.append(simpleDateFormat.format(date))
                .append(",").append(query2Outcome.getSeaType());

        builder.append(",00:00-11:59,[");

        List<Query2IntermediateOutcome> temporaryList = query2Outcome.getAmTop3();
        Collections.reverse(temporaryList);
        int iterations = temporaryList.size();
        for(int i = 0; i<iterations; i++){
            if(i+1==iterations)
                builder.append(temporaryList.get(i).getCellId());
            else
                builder.append(temporaryList.get(i).getCellId()).append(";");
        }


        builder.append("],12:00-23:59,[");
        temporaryList = query2Outcome.getPmTop3();
        Collections.reverse(temporaryList);
        iterations = temporaryList.size();
        for(int i = 0; i<iterations; i++){
            if(i+1==iterations)
                builder.append(temporaryList.get(i).getCellId());
            else
                builder.append(temporaryList.get(i).getCellId()).append(";");
        }
        builder.append("]");

        return builder.toString();
    }

}
