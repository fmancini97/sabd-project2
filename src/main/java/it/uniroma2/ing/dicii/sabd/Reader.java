package it.uniroma2.ing.dicii.sabd;

import it.uniroma2.ing.dicii.sabd.flink.query1.Query1Structure;
import it.uniroma2.ing.dicii.sabd.flink.query2.Query2Structure;
import it.uniroma2.ing.dicii.sabd.flink.query3.Query3Structure;
import it.uniroma2.ing.dicii.sabd.utils.KafkaProperties;
import it.uniroma2.ing.dicii.sabd.utils.TimeIntervalEnum;
import org.apache.kafka.clients.consumer.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class Reader {

    private static final String query1Header = "ts,id_cella,ship_t35,agv_t35,ship_t60-69,agv_t60-69,ship_t70-79,agv_t70-79,ship_to,agv_to\n";
    private static final String query2Header = "ts,sea,slot_a,rank_a,slot_p,rank_p\n";
    private static final String query3Header = "ts,trip_1,rating_1,trip_2,rating_2,trip_3,rating_3,trip_4,rating_4,trip_5,rating_5\n";

    public static void main(String[] args) {


        Properties props = KafkaProperties.getCSVWriterProperties();
        Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        Map<String, FileWriter> topicWriterMap = new HashMap<>();
        Collection<String> topics = new ArrayList<>();

        for(TimeIntervalEnum timeIntervalEnum: TimeIntervalEnum.values()){
            try {
                String topic;
                FileWriter fileWriter;
                switch (timeIntervalEnum) {
                    case HOURLY:
                    case EVERYTWOHOURS:
                        topic = KafkaProperties.QUERY3_TOPIC + timeIntervalEnum.getTimeIntervalName();
                        topics.add(topic);
                        fileWriter = new FileWriter(topic + ".csv");
                        fileWriter.write(query3Header);
                        fileWriter.flush();
                        topicWriterMap.put(topic, fileWriter);
                        break;
                    case WEEKLY:
                    case MONTHLY:
                        topic = KafkaProperties.QUERY1_TOPIC + timeIntervalEnum.getTimeIntervalName();
                        topics.add(topic);
                        fileWriter = new FileWriter(topic + ".csv");
                        fileWriter.write(query1Header);
                        fileWriter.flush();
                        topicWriterMap.put(topic, fileWriter);

                        topic = KafkaProperties.QUERY2_TOPIC + timeIntervalEnum.getTimeIntervalName();
                        topics.add(topic);
                        fileWriter = new FileWriter(topic + ".csv");
                        fileWriter.write(query2Header);
                        fileWriter.flush();
                        topicWriterMap.put(topic, fileWriter);
                        break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        consumer.subscribe(topics);

        while (true) {
            //todo ragionare sulla durata del poll addatta
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            if (consumerRecords.count() == 0) {
                System.out.println("No record");
            } else {
                consumerRecords.forEach(longStringConsumerRecord -> {
                    try {
                        FileWriter fileWriter = topicWriterMap.get(longStringConsumerRecord.topic());
                        fileWriter.write(longStringConsumerRecord.value());
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
