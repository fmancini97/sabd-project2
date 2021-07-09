package it.uniroma2.ing.dicii.sabd.utils.timeIntervals;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;


/**
 * 2 Hours Tumbling Event Time Window aligned to first minute of the first hour
 */
public class EveryTwoHoursWindowAssigner extends TumblingEventTimeWindows {

    public EveryTwoHoursWindowAssigner(){
        super(1,0, WindowStagger.ALIGNED);
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object o, long timestamp, WindowAssignerContext context){
        Calendar calendar = Calendar.getInstance();

        calendar.setTime(new Date(timestamp));
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        long startDate = calendar.getTimeInMillis();

        calendar.add(Calendar.HOUR_OF_DAY,2);

        long endDate = calendar.getTimeInMillis()-1;

        return Collections.singletonList(new TimeWindow(startDate, endDate));

    }
}
