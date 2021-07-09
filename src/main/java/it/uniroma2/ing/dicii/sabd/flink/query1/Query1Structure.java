package it.uniroma2.ing.dicii.sabd.flink.query1;

import it.uniroma2.ing.dicii.sabd.data.TripData;
import it.uniroma2.ing.dicii.sabd.data.ShipTypeEnum;
import it.uniroma2.ing.dicii.sabd.kafka.KafkaProperties;
import it.uniroma2.ing.dicii.sabd.utils.timeIntervals.TimeIntervalEnum;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;


public class Query1Structure {

    public static void build(DataStream<TripData> stream, TimeIntervalEnum timeIntervalEnum) throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        Constructor<? extends TumblingEventTimeWindows> timeIntervalConstructor = null;

        timeIntervalConstructor = timeIntervalEnum.getTimeIntervalClass().getConstructor();


        Properties props = KafkaProperties.getFlinkProducerProperties();

        SingleOutputStreamOperator<String> resultStream = stream.keyBy(TripData::getCell)
                .window(timeIntervalConstructor.newInstance())
                .aggregate(new Query1Aggregator(), new Query1Window())
                .name("query1" + timeIntervalEnum.getTimeIntervalName())
                .map((MapFunction<Query1Outcome, String>) query1Outcome -> {
                    return query1OutcomeToResultMap(timeIntervalEnum,query1Outcome);
                });

        resultStream.addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY1_TOPIC + timeIntervalEnum.getTimeIntervalName(),
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


        for(ShipTypeEnum shipType: ShipTypeEnum.values()){
            Integer v = query1Outcome.getTypeMap().get(shipType.getShipType());
            if(v == null){
                builder.append(",").append(shipType.getShipType()).append(",").append("");
            } else {
                builder.append(",").append(shipType.getShipType()).append(",").append(String.format(Locale.ENGLISH, "%.2g",(double)v/daysOfActualTimeInterval));
            }
        }

        return builder.toString();
    }

}
