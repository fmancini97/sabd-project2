package it.uniroma2.ing.dicii.sabd.utils;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

public class MonthlyWindowAssigner extends TumblingEventTimeWindows {

    public MonthlyWindowAssigner(){
        super(1,0, WindowStagger.ALIGNED);
    }
    /*
    protected MonthlyWindowAssigner(long size, long offset, WindowStagger windowStagger) {
        super(size, offset, windowStagger);
    }
    */
    @Override
    public Collection<TimeWindow> assignWindows(Object o, long timestamp, WindowAssignerContext context){
        Calendar calendar = Calendar.getInstance();

        calendar.setTime(new Date(timestamp));
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);

        long startDate = calendar.getTimeInMillis();

        calendar.add(Calendar.MONTH,1);

        long endDate = calendar.getTimeInMillis()-1;

        return Collections.singletonList(new TimeWindow(startDate, endDate));

    }
}
