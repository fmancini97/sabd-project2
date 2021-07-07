package it.uniroma2.ing.dicii.sabd.kafkastreams.query1;

import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.util.Map;

public abstract class CustomWindow extends Windows<TimeWindow> {


    @Override
    public Map<Long, TimeWindow> windowsFor(long l) {
        return null;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public long gracePeriodMs() {
        return 0;
    }
}
