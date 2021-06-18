package it.uniroma2.ing.dicii.sabd;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 *
 */
public class Injector
{
    public static void main( String[] args ) {

        Long range = 30 * 60000L;

        SimpleDateFormat[] dateFormats = {new SimpleDateFormat("dd/MM/yy HH:mm"),
                new SimpleDateFormat("dd-MM-yy HH:mm")};

        Properties props = new Properties();
        // specify brokers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // set consumer group id
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "flink-producer");
        // start reading from beginning of partition if no offset was created
        // exactly once semantic
        // key and value deserializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());



        System.out.println("Reading file");
        BufferedReader reader = null;
        Map<Long, List<String>> map = new TreeMap<>();
        Long min = Long.MAX_VALUE;
        Long max = Long.MIN_VALUE;



        try {
            reader = new BufferedReader(new FileReader("data/dataset.csv"));
            String line;
            reader.readLine(); //read header
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                String dateString = values[7];
                Long timestamp = null;
                for (SimpleDateFormat dateFormat: dateFormats) {
                    try {
                        timestamp = dateFormat.parse(dateString).getTime();
                        break;
                    } catch (ParseException ignored) { }
                }


                if (timestamp == null) {
                    System.out.println("Erroreeeeee!!!!!");
                    return;
                }

                System.out.println(timestamp);

                min = (min < timestamp) ? min : timestamp;
                max = (max > timestamp) ? max : timestamp;

                List<String> records = map.computeIfAbsent(timestamp, k -> new ArrayList<>());
                records.add(line);

            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Double proportion = range / (double) (max - min);

        Producer<Long, String> producer = new KafkaProducer<>(props);

        Set<Map.Entry<Long, List<String>>> timeSet = map.entrySet();


        Long previous = null;
        System.out.println(max - min);
        Long sum = 0L;

        for (Map.Entry<Long, List<String>> entry: timeSet) {
            Long timestamp = entry.getKey();

            if (previous != null) {
                try {


                    long sleepTime =  (long) ((timestamp - previous) * proportion);
                    sum += sleepTime;
                    System.out.println(sleepTime);

                    TimeUnit.MILLISECONDS.sleep(sleepTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println(timestamp);


            ProducerRecord<Long, String> producerRecord = new ProducerRecord<>("flink", timestamp, "ciao");

            producer.send(producerRecord, (recordMetadata, e) -> {e.printStackTrace();});
            /*
            for (String record : entry.getValue()) {

                ProducerRecord<Long, String> producerRecord = new ProducerRecord<>("flink", timestamp, record);

                System.out.println(record);
                producer.send(producerRecord, (recordMetadata, e) -> {e.printStackTrace();});
            }*/
            producer.flush();

            previous = timestamp;
        }

        System.out.println( "Hello World! " + sum );
    }
}
