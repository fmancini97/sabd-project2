package it.uniroma2.ing.dicii.sabd.utils;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.util.Calendar;
import java.util.Date;

public enum TimeIntervalEnum {

    HOURLY("Hourly", HourlyWindowAssigner.class,Calendar.HOUR_OF_DAY),
    EVERYTWOHOURS("EveryTwoHours", HourlyWindowAssigner.class,Calendar.HOUR_OF_DAY),
    WEEKLY("Weekly", WeeklyWindowAssigner.class,Calendar.DAY_OF_WEEK),
    MONTHLY("Monthly", MonthlyWindowAssigner.class,Calendar.DAY_OF_MONTH);

    private final String timeIntervalName;
    private final Class<? extends TumblingEventTimeWindows> timeIntervalClass;

    private final int dayOfActualTimeInterval;

    TimeIntervalEnum(final String timeIntervalName, final Class<? extends TumblingEventTimeWindows> timeIntervalClass,
                     int dayOfActualTimeInterval){
        this.timeIntervalName = timeIntervalName;
        this.timeIntervalClass = timeIntervalClass;
        this.dayOfActualTimeInterval = dayOfActualTimeInterval;
    }

    public String getTimeIntervalName() {
        return timeIntervalName;
    }

    public Class<? extends TumblingEventTimeWindows> getTimeIntervalClass() {
        return timeIntervalClass;
    }

    public int getNumDays(Date date){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        return calendar.getActualMaximum(this.dayOfActualTimeInterval);
    }
}