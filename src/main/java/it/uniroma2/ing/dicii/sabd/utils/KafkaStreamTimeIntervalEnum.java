package it.uniroma2.ing.dicii.sabd.utils;

import it.uniroma2.ing.dicii.sabd.kafkastreams.query1.MonthlyWindow;
import it.uniroma2.ing.dicii.sabd.kafkastreams.query1.WeeklyWindow;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.util.Calendar;
import java.util.Date;

public enum KafkaStreamTimeIntervalEnum {

    WEEKLY("Weekly", WeeklyWindow.class, Calendar.DAY_OF_WEEK),
    MONTHLY("Monthly",MonthlyWindow .class, Calendar.DAY_OF_MONTH);

    private final String timeIntervalName;
    private final Class<? extends Windows<TimeWindow>> timeIntervalClass;

    private final int dayOfActualTimeInterval;

    private KafkaStreamTimeIntervalEnum(final String timeIntervalName, final Class<? extends Windows<TimeWindow>> timeIntervalClass,
                             int dayOfActualTimeInterval){
        this.timeIntervalName = timeIntervalName;
        this.timeIntervalClass = timeIntervalClass;
        this.dayOfActualTimeInterval = dayOfActualTimeInterval;
    }

    public String getTimeIntervalName() {
        return timeIntervalName;
    }

    public Class<? extends Windows<TimeWindow>> getTimeIntervalClass() {
        return timeIntervalClass;
    }

    public int getNumDays(Date date){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        return calendar.getActualMaximum(this.dayOfActualTimeInterval);
    }

}
