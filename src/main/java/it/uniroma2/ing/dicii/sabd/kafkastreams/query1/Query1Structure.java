package it.uniroma2.ing.dicii.sabd.kafkastreams.query1;

import it.uniroma2.ing.dicii.sabd.TripData;
import it.uniroma2.ing.dicii.sabd.flink.query1.Query1Accumulator;
import it.uniroma2.ing.dicii.sabd.flink.query1.Query1Outcome;
import it.uniroma2.ing.dicii.sabd.utils.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;


public class Query1Structure {

    public static void build(KStream<Long, TripData> stream, KafkaStreamTimeIntervalEnum timeIntervalEnum) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {

        Constructor<? extends Windows<TimeWindow>> timeIntervalConstructor = null;
        timeIntervalConstructor = timeIntervalEnum.getTimeIntervalClass().getConstructor();


        stream.map((KeyValueMapper<Long, TripData, KeyValue<String, TripData>>) (aLong, tripData) -> {
                    return  new KeyValue<String, TripData>(tripData.getCell(), tripData);
                })
                .groupByKey(Grouped.with(Serdes.String(), SerDesBuilder.getSerdes(TripData.class)))
                // used a custom daily window
                .windowedBy(timeIntervalConstructor.newInstance())
                // set up function to aggregate weekly data for average delay
                .aggregate(new Query1Initializer(), new Query1Aggregator(),
                        Materialized.with(Serdes.String(), SerDesBuilder.getSerdes(Query1Accumulator.class)))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                // parse the aggregate outcome to a string
                .map(new KeyValueMapper<Windowed<String>, Query1Accumulator, KeyValue<String, String>>() {
                    @Override
                    public KeyValue<String, String> apply(Windowed<String> stringWindowed, Query1Accumulator query1Accumulator) {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

                        StringBuilder builder = new StringBuilder();
                        Date date = new Date(stringWindowed.window().startTime().toEpochMilli());

                        Query1Outcome query1Outcome = new Query1Outcome(query1Accumulator.getTypeMap()); //todo si può eliminare, ma funziona così

                        builder.append(simpleDateFormat.format(date))
                                .append(",").append(query1Outcome.getCellId());

                        int daysOfActualTimeInterval = timeIntervalEnum.getNumDays(date);
                        System.out.println(daysOfActualTimeInterval);

                        for(ShipTypeEnum shipType: ShipTypeEnum.values()){
                            Integer v = query1Outcome.getTypeMap().get(shipType.getShipType());
                            if(v == null){
                                builder.append(",").append(shipType.getShipType()).append(",").append("");
                            } else {
                                builder.append(",").append(shipType.getShipType()).append(",").append(String.format(Locale.ENGLISH, "%.2g",(double)v/daysOfActualTimeInterval));
                            }
                        }

                        return new KeyValue<>(stringWindowed.key(), builder.toString());
                    }
                })
                // publish results to the correct kafka topic
                .to(KafkaProperties.QUERY1_TOPIC+"KafkaStream" +timeIntervalEnum.getTimeIntervalName() + "Sink", Produced.with(Serdes.String(), Serdes.String()));



    }

}
