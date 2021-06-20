package it.uniroma2.ing.dicii.sabd;

import it.uniroma2.ing.dicii.sabd.Utils.KafkaProperties;
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


        Properties props = KafkaProperties.getCSVWriterProperties();

        Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(KafkaProperties.QUERY1_WEEKLY_TOPIC));
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
