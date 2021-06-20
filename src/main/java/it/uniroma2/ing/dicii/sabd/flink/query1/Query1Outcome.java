package it.uniroma2.ing.dicii.sabd.flink.query1;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Query1Outcome {

    private HashMap<String, Map<String, Integer>> cellsMap;
    Date date;

    public void setDate(Date date){
        this.date = date;
    }

    public Date getDate(){
        return date;
    }

    public HashMap<String, Map<String, Integer>> getCellsMap() {
        return cellsMap;
    }

    public Query1Outcome(){
        this.cellsMap = new HashMap<>();
    }

    public Query1Outcome(HashMap<String, Map<String, Set<String>>> cellsMapInput){
        this.cellsMap = new HashMap<>();
        for(String cell: cellsMapInput.keySet()){
            for(String shipType: cellsMapInput.get(cell).keySet()){
               HashMap<String,Integer> shipTypeCounterMap = new HashMap<>();
               shipTypeCounterMap.put(shipType, cellsMapInput.get(cell).get(shipType).size());
               cellsMap.put(cell, shipTypeCounterMap);
            }
        }
    }

}
