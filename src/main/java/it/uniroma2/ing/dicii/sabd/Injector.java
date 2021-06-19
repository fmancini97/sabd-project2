package it.uniroma2.ing.dicii.sabd;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Hello world!
 *
 */
public class Injector {
    private static final SimpleDateFormat[] dateFormats = {new SimpleDateFormat("dd/MM/yy HH:mm"),
            new SimpleDateFormat("dd-MM-yy HH:mm")};
    private static final String bootstrapServer = "kafka:9092";
    private static final String producerId = "dataInjector";
    private static final String topic = "flink";
    private static final String datasetPath = "/data/dataset.csv";


    public static void main( String[] args ) {

        Logger log = Logger.getLogger(Injector.class.getSimpleName());

        log.info("Parsing parameter");

        if (args.length == 0) {
            log.log(Level.WARNING, "Usage: {0} <minutes>", Injector.class.getName());
            System.exit(-1);
        }

        long timeRange = 0;
        try {
            timeRange = Long.parseLong(args[0]);
        } catch (NumberFormatException e) {
            log.log(Level.WARNING, "Wrong number format: {0}", args[0]);
            System.exit(-1);
        }

        log.log(Level.INFO, "Starting Injector with timeRange = {0} minutes", timeRange);

        // Converting timeRange from minutes to milliseconds
        timeRange = timeRange * 60 * 1000;

        log.info("Reading data from file");
        Map<Long, List<String>> map = new TreeMap<>();
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;

        try {
            BufferedReader reader = new BufferedReader(new FileReader(Injector.datasetPath));
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
                    log.log(Level.SEVERE, "Unable to parse the date: {0}", dateString);
                    System.exit(-1);
                }

                min = (min < timestamp) ? min : timestamp;
                max = (max > timestamp) ? max : timestamp;
                List<String> records = map.computeIfAbsent(timestamp, k -> new ArrayList<>());
                records.add(line);
            }
            reader.close();
        } catch (IOException e) {
            log.log(Level.SEVERE, "Error while reading file: {0}", e.getMessage());
        }

        Properties props = new Properties();
        // specify brokers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Injector.bootstrapServer);
        // set consumer group id
        props.put(ProducerConfig.CLIENT_ID_CONFIG, Injector.producerId);
        // key and value deserializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<Long, String> producer = new KafkaProducer<>(props);

        Set<Map.Entry<Long, List<String>>> timeSet = map.entrySet();

        double proportion = timeRange / (double) (max - min);
        long recordsSent = 0L;
        Long previous = null;

        log.info("Starting sending records");
        for (Map.Entry<Long, List<String>> entry: timeSet) {
            Long timestamp = entry.getKey();

            if (previous != null) {
                try {
                    long sleepTime =  (long) ((timestamp - previous) * proportion);
                    TimeUnit.MILLISECONDS.sleep(sleepTime);
                } catch (InterruptedException e) {
                    log.log(Level.WARNING, "Error while thread was sleeping: {0}", e.getMessage());
                }
            }

            for (String record : entry.getValue()) {
                ProducerRecord<Long, String> producerRecord = new ProducerRecord<>(Injector.topic, null,
                        timestamp, timestamp, record);
                producer.send(producerRecord);
                recordsSent += 1;
                if (recordsSent % 500 == 0) log.log(Level.INFO, "{0} records sent", recordsSent);
            }
            previous = timestamp;
        }

        producer.flush();
        producer.close();
        log.info("Records sent");

    }
}
