package it.uniroma2.ing.dicii.sabd;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.security.cert.TrustAnchor;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Reader {

    public static void main(String[] args) {


        Properties props = new Properties();


        // specify brokers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // set consumer group id
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer");
        // start reading from beginning of partition if no offset was created
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // exactly once semantic

        // key and value deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Consumer<Long, String> consumer = new KafkaConsumer<Long, String>(props);
        consumer.subscribe(Collections.singletonList("flink"));
        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            if (consumerRecords.count() == 0) {
                System.out.println("No record");
            } else {
                consumerRecords.forEach(longStringConsumerRecord -> System.out.println(longStringConsumerRecord.toString()));
            }
            consumer.commitAsync();
        }

    }
}
