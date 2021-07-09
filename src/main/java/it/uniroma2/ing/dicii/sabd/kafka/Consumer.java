package it.uniroma2.ing.dicii.sabd.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;


/**
 * It writes each record in input on a different file based on the topic from which it has read it
 */
public class Consumer {

    private static final String query1Header = "ts,id_cella,ship_t35,agv_t35,ship_t60-69,agv_t60-69,ship_t70-79,agv_t70-79,ship_to,agv_to\n";
    private static final String query2Header = "ts,sea,slot_a,rank_a,slot_p,rank_p\n";
    private static final String query3Header = "ts,trip_1,rating_1,trip_2,rating_2,trip_3,rating_3,trip_4,rating_4,trip_5,rating_5\n";
    private static final Map<String, String> headers;
    private static final String outputPath = "/output";

    static {
        headers = new HashMap<>();
        headers.put(KafkaProperties.QUERY1_TOPIC, query1Header);
        headers.put(KafkaProperties.QUERY2_TOPIC, query2Header);
        headers.put(KafkaProperties.QUERY3_TOPIC, query3Header);
    }

    public static void main(String[] args) {
        Logger log = Logger.getLogger(Consumer.class.getSimpleName());

        Properties props = KafkaProperties.getCSVWriterProperties();
        org.apache.kafka.clients.consumer.Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        Map<String, FileWriter> topicWriterMap = new HashMap<>();
        consumer.subscribe(Pattern.compile("^(query).*$"));

        log.info("Starting receiving records");
        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            if (consumerRecords.count() == 0) {
                log.fine("No records");
            } else {
                consumerRecords.forEach(longStringConsumerRecord -> {
                    try {
                        String topic = longStringConsumerRecord.topic();
                        log.log(Level.FINER, "Topic: {0} - Record: {1}",
                                Arrays.asList(topic, longStringConsumerRecord.value()));
                        FileWriter fileWriter = topicWriterMap.get(topic);
                        if (fileWriter == null) {
                            log.log(Level.INFO, "New topic: {0}", topic);
                            fileWriter = new FileWriter(outputPath + "/" + topic + ".csv", false);
                            fileWriter.write(headers.get(topic.substring(0,6)));
                            fileWriter.flush();
                            topicWriterMap.put(topic, fileWriter);
                        }
                        fileWriter.write(longStringConsumerRecord.value() + "\n");

                        fileWriter.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
            consumer.commitAsync();
        }
    }
}
