package it.uniroma2.ing.dicii.sabd.flink.query3;

import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


public class SessionWindowTrigger extends Trigger<Object, TimeWindow> {

    private final EventTimeTrigger eventTimeTrigger;

    private SessionWindowTrigger(){
        this.eventTimeTrigger = EventTimeTrigger.create();
    }

    @Override
    public TriggerResult onElement(Object o, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        TriggerResult result = this.eventTimeTrigger.onElement(o, l, timeWindow, triggerContext);
        return (result == TriggerResult.FIRE) ? TriggerResult.FIRE_AND_PURGE : TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return eventTimeTrigger.onProcessingTime(l, timeWindow, triggerContext);
    }

    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        TriggerResult result = this.eventTimeTrigger.onEventTime(l, timeWindow, triggerContext);
        return (result == TriggerResult.FIRE) ? TriggerResult.FIRE_AND_PURGE : TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        this.eventTimeTrigger.clear(timeWindow, triggerContext);
    }

    @Override
    public boolean canMerge() {
        return this.eventTimeTrigger.canMerge();
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
        this.eventTimeTrigger.onMerge(window, ctx);
    }

    public static SessionWindowTrigger create() {
        return new SessionWindowTrigger();
    }
}
