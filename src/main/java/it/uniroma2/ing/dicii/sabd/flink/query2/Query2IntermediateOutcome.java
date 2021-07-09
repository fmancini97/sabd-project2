package it.uniroma2.ing.dicii.sabd.flink.query2;

import it.uniroma2.ing.dicii.sabd.data.GridHandler;
import it.uniroma2.ing.dicii.sabd.data.GridHandler.Sea;

import java.util.*;

/**
 * It maintains the attendance of a single cell
 */
public class Query2IntermediateOutcome {


    public int getAttendance() {
        return attendance;
    }

    public void setAttendance(int attendance) {
        this.attendance = attendance;
    }

    private int attendance;

    private String cellId;
    private Date date;
    private Sea seaType;

    public Sea getSeaType(){
        return seaType;
    }



    public Query2IntermediateOutcome(int attendance){
        this.attendance = attendance;
    }

    private void setSeaType(){
        int lonId = Integer.parseInt(cellId.substring(1));
        if(lonId <= GridHandler.lonIndexSeaSeparator){
            seaType = Sea.Occidentale;
        } else {
            seaType = Sea.Orientale;
        }
    }

    public String getCellId() {
        return cellId;
    }

    public void setCellId(String cellId) {
        this.cellId = cellId;
        setSeaType();
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Query2IntermediateOutcome(){
    }

    public int getHourInDate(){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    public Query2IntermediateOutcome(Set<String> attenanceInput){
        attendance = attenanceInput.size();
    }

}
