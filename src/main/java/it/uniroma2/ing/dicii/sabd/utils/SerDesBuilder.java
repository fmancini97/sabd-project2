package it.uniroma2.ing.dicii.sabd.utils;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import java.util.HashMap;
import java.util.Map;

public class SerDesBuilder {
    public static <T> Serde<T> getSerdes(Class<T> classT) {
        Map<String, Object> serdeProps = new HashMap<>();

        // create serializer and deserializer
        Serializer<T> serializer = new JsonPOJOSerializer<>();;
        Deserializer<T> deserializer = new JsonPOJODeserializer<>();
        // specify the class as input of serialization and output of deserialization
        serdeProps.put("JsonPOJOClass", classT);
        serializer.configure(serdeProps, false);
        serdeProps.put("JsonPOJOClass", classT);
        deserializer.configure(serdeProps, false);

        // create serializer-deserializer
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
