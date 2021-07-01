package it.uniroma2.ing.dicii.sabd.flink.query3;

import it.uniroma2.ing.dicii.sabd.TripData;
import it.uniroma2.ing.dicii.sabd.flink.query1.FlinkOutputSerializer;
import it.uniroma2.ing.dicii.sabd.utils.KafkaProperties;
import it.uniroma2.ing.dicii.sabd.utils.TimeIntervalEnum;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class Query3Structure {


    public static void build(DataStream<Tuple2<Long, String>> source, TimeIntervalEnum timeIntervalEnum) throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {

        //Constructor<? extends TumblingEventTimeWindows> timeIntervalConstructor = null;

        //timeIntervalConstructor = timeIntervalEnum.getTimeIntervalClass().getConstructor();


        DataStream<TripData> stream = source.map((MapFunction<Tuple2<Long, String>, TripData>) tuple -> {
            String[] info = tuple.f1.split(",");
            return new TripData(info[10],info[0],Double.parseDouble(info[3]),
                    Double.parseDouble(info[4]), tuple.f0, Integer.parseInt(info[1]), tuple.f0);
        }).name("stream-query3");

        Properties props = KafkaProperties.getFlinkProducerProperties();

        stream.keyBy(TripData::getTripId)
                .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
                .trigger(SessionWindowTrigger.create())
                .aggregate(new Query3Aggregator(), new Query3Window())
                .name("query2HalfDay")
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new Query3Ranking(), new Query3RankWindow())
                .print();
                /*.addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY2_TOPIC + timeIntervalEnum.getTimeIntervalName(),
                        new FlinkOutputSerializer(KafkaProperties.QUERY2_TOPIC + timeIntervalEnum.getTimeIntervalName()),
                        props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query2" + timeIntervalEnum.getTimeIntervalName() + "Sink");*/

    }

}
