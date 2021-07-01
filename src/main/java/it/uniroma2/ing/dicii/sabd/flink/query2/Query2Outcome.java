package it.uniroma2.ing.dicii.sabd.flink.query2;

import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class Query2Outcome {

    private Date date;

    private List<Query2IntermediateOutcome> amTop3;
    private List<Query2IntermediateOutcome> pmTop3;
    private String seaType;

    public Query2Outcome(){
        amTop3 = new ArrayList<>();
        pmTop3 = new ArrayList<>();
        for(int i = 0; i < 3; i++){
            Query2IntermediateOutcome query2IntermediateOutcome = new Query2IntermediateOutcome(-1);
            this.forceAdd(query2IntermediateOutcome);
        }
    }

    public List<Query2IntermediateOutcome> getAmTop3() {
        return amTop3;
    }

    public void setAmTop3(List<Query2IntermediateOutcome> amTop3) {
        this.amTop3 = amTop3;
    }

    public List<Query2IntermediateOutcome> getPmTop3() {
        return pmTop3;
    }

    public void setPmTop3(List<Query2IntermediateOutcome> pmTop3) {
        this.pmTop3 = pmTop3;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }


    public String getSeaType() {
        return seaType;
    }

    public void setSeaType(String seaType) {
        this.seaType = seaType;
    }


    public void forceAdd(Query2IntermediateOutcome query2IntermediateOutcome){
        amTop3.add(query2IntermediateOutcome);
        pmTop3.add(query2IntermediateOutcome);
    }

    public void addAM(Query2IntermediateOutcome intermediateOutcome){
        int actualAmAttendace = intermediateOutcome.getAttendance();
        if(actualAmAttendace > amTop3.get(1).getAttendance()) {
            if(actualAmAttendace > amTop3.get(2).getAttendance()){
                amTop3.add(3,intermediateOutcome);
                amTop3.remove(0);
            } else {
                amTop3.add(2, intermediateOutcome);
                amTop3.remove(0);
            }
        } else {
            if(actualAmAttendace > amTop3.get(0).getAttendance()){
                amTop3.add(1,intermediateOutcome);
                amTop3.remove(0);
            }
        }
    }

    public void addPM(Query2IntermediateOutcome intermediateOutcome){
        int actualPmAttendace = intermediateOutcome.getAttendance();
        if(actualPmAttendace > pmTop3.get(1).getAttendance()) {
            if(actualPmAttendace > pmTop3.get(2).getAttendance()){
                pmTop3.add(3,intermediateOutcome);
                pmTop3.remove(0);
            } else {
                pmTop3.add(2, intermediateOutcome);
                pmTop3.remove(0);
            }
        } else {
            if(actualPmAttendace > pmTop3.get(0).getAttendance()){
                pmTop3.add(1,intermediateOutcome);
                pmTop3.remove(0);
            }
        }
    }

    public void add(Query2IntermediateOutcome intermediateOutcome){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(intermediateOutcome.getDate());
        int hourOfDay = calendar.get(Calendar.HOUR_OF_DAY);
        if(hourOfDay < 12){
            addAM(intermediateOutcome);
        } else {
            addPM(intermediateOutcome);
        }

    }
}
