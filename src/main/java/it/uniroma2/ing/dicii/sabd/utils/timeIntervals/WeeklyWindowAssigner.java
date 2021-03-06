package it.uniroma2.ing.dicii.sabd.utils.timeIntervals;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

/**
 * Weekly Tumbling Event Time Window aligned to first day of the week
 */
public class WeeklyWindowAssigner extends TumblingEventTimeWindows {

    public WeeklyWindowAssigner(){
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
        calendar.set(Calendar.DAY_OF_WEEK, 1);

        long startDate = calendar.getTimeInMillis();

        calendar.add(Calendar.WEEK_OF_YEAR,1);

        long endDate = calendar.getTimeInMillis()-1;

        return Collections.singletonList(new TimeWindow(startDate, endDate));

    }
}
