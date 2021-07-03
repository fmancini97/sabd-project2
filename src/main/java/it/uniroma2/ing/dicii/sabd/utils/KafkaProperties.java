package it.uniroma2.ing.dicii.sabd.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProperties {


    public static final String SOURCE_TOPIC = "source";
    public static final String QUERY1_TOPIC = "query1";
    public static final String QUERY2_TOPIC = "query2";
    public static final String KAFKA_ADDRESS = "kafka:9092";

    public static Properties getInjectorProperties(){
        Properties props = new Properties();
        // specify brokers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ADDRESS);
        // set consumer group id
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "flink-producer");
        // start reading from beginning of partition if no offset was created
        // exactly once semantic
        // key and value deserializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    public static Properties getFlinkConsumerProperties(){
        Properties props = new Properties();
        // specify brokers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ADDRESS);
        // set consumer group id
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flink1");
        // start reading from beginning of partition if no offset was created
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // exactly once semantic
        //props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, true);

        // key and value deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }

    public static Properties getFlinkProducerProperties(){
        Properties props = new Properties();
        // specify brokers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ADDRESS);
        // set producer id
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
        // exactly once semantic
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return props;
    }

    public static Properties getCSVWriterProperties(){
        Properties props = new Properties();
        // specify brokers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ADDRESS);
        // set consumer group id
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer");
        // start reading from beginning of partition if no offset was created
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // exactly once semantic

        // key and value deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }

}
