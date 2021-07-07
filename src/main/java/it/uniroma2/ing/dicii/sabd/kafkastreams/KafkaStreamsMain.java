package it.uniroma2.ing.dicii.sabd.kafkastreams;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import it.uniroma2.ing.dicii.sabd.TripData;
import it.uniroma2.ing.dicii.sabd.kafkastreams.query1.Query1Structure;
import it.uniroma2.ing.dicii.sabd.utils.KafkaProperties;
import it.uniroma2.ing.dicii.sabd.utils.KafkaStreamsProperties;
import it.uniroma2.ing.dicii.sabd.utils.TimeIntervalEnum;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

public class KafkaStreamsMain {

    static SimpleDateFormat[] dateFormats = {new SimpleDateFormat("dd/MM/yy HH:mm"),
            new SimpleDateFormat("dd-MM-yy HH:mm")};

    public static void main(String[] args) {

        // create kafka streams properties
        Properties props = KafkaStreamsProperties.createStreamProperties();
        StreamsBuilder builder = new StreamsBuilder();


        // define input
        KStream<Long,String> inputStream = builder.stream(KafkaProperties.SOURCE_TOPIC);

        KStream<Long,TripData> dataStream = inputStream.mapValues(new ValueMapper<String, TripData>() {
            @Override
            public TripData apply(String s) {
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
        });

        // build query 1 topology
        for(TimeIntervalEnum timeIntervalEnum: TimeIntervalEnum.values()){
            switch (timeIntervalEnum){
                case HOURLY:
                case EVERYTWOHOURS:
                    break;
                case WEEKLY:
                case MONTHLY:
                     Query1Structure.build(dataStream,timeIntervalEnum);
                    break;
            }
        }





        // build, cleanup and start
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
