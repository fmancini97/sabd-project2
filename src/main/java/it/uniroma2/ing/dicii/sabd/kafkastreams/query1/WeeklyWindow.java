package it.uniroma2.ing.dicii.sabd.kafkastreams.query1;

import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class WeeklyWindow extends Windows<TimeWindow> {


    @Override
    public Map<Long, TimeWindow> windowsFor(long l) {

        // get first instant of the current week (monday-sunday)
        Calendar calendar = Calendar.getInstance(); //todo final
        calendar.setTime(new Date(l));
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.DAY_OF_WEEK, 1);

        long startTime = calendar.getTimeInMillis();  //todo final

        calendar.add(Calendar.WEEK_OF_YEAR, 1);

        long endTime = calendar.getTimeInMillis() - 1;  //todo final

        Map<Long, TimeWindow> windows = new LinkedHashMap<>(); //todo final
        windows.put(startTime, new TimeWindow(startTime, endTime));
        return windows;
    }

    @Override
    public long size() {
        return Duration.ofDays(7L).toMillis();
    }

    @Override
    public long gracePeriodMs() {
        return Duration.ofHours(2L).toMillis();
    }
}
