package it.uniroma2.ing.dicii.sabd.utils;

import it.uniroma2.ing.dicii.sabd.flink.query1.FlinkOutputSerializer;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Date;
import java.util.Properties;

public class MetricsGenerator<T> {

    public void generateMetricSink(String topic, SingleOutputStreamOperator<T> stream, Properties props) {

        stream.map(new ComputeThroughputAndLatency<>())
                .addSink(new FlinkKafkaProducer<>(topic + "Metrics",
                        new FlinkOutputSerializer(topic + "Metrics"),
                        props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).setParallelism(1);
    }


    public static class ComputeThroughputAndLatency<T> extends RichMapFunction<T, String> {

        private transient ValueState<Tuple2<Long, Long>> timestampAndCounter;

        @Override
        public String map(Object o) throws Exception {
            StringBuilder outputBuilder = new StringBuilder();
            Tuple2<Long, Long> currentState = this.timestampAndCounter.value();
            if (currentState == null) {
                currentState = new Tuple2<>(new Date().getTime(), 0L);
            }

            currentState.f1 += 1;

            this.timestampAndCounter.update(currentState);


            //return new Tuple2<>(currentState.f1/currentState.f0, currentState.f0/currentState.f1);

            return outputBuilder.append(currentState.f1/ currentState.f0).append(",").append(currentState.f0/currentState.f1).toString();

        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                    new ValueStateDescriptor<>(
                            "metric", // the state name
                            TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}));
            this.timestampAndCounter = getRuntimeContext().getState(descriptor);
        }

    }

}
