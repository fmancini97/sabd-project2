package it.uniroma2.ing.dicii.sabd.flink.query1;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class FlinkOutputSerializer implements KafkaSerializationSchema<String> {


    private final String topic;

    public FlinkOutputSerializer(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String value, @Nullable Long aLong) {
        return new ProducerRecord<>(topic, value.getBytes(StandardCharsets.UTF_8));
    }
}
