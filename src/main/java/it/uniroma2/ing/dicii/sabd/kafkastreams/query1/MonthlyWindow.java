package it.uniroma2.ing.dicii.sabd.kafkastreams.query1;

import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class MonthlyWindow extends Windows<TimeWindow> {

    @Override
    public Map<Long, TimeWindow> windowsFor(long l) {

        Calendar calendar = Calendar.getInstance();

        calendar.setTime(new Date(l));
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);

        long startDate = calendar.getTimeInMillis();

        calendar.add(Calendar.MONTH,1);

        long endDate = calendar.getTimeInMillis()-1;

        Map<Long, TimeWindow> windows = new LinkedHashMap<>();
        windows.put(startDate, new TimeWindow(startDate, endDate));
        return windows;
    }

    @Override
    public long size() {
        return Duration.ofDays(31L).toMillis(); //maximum size 31 days
    }

    @Override
    public long gracePeriodMs() {
        return Duration.ofHours(2L).toMillis();
    }
}
