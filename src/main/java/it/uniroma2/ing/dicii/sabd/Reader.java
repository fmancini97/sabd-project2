package it.uniroma2.ing.dicii.sabd;

import it.uniroma2.ing.dicii.sabd.utils.KafkaProperties;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Reader {

    public static void main(String[] args) {


        Properties props = KafkaProperties.getCSVWriterProperties();

        Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(KafkaProperties.QUERY2_TOPIC+ "Weekly"));
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
