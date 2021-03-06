package it.uniroma2.ing.dicii.sabd.flink.query1;

import java.util.*;

/**
 * For each cell, it maintains the average number of trips over the cell based on the ship type
 */
public class Query1Outcome {

    private HashMap<String, Integer> typeMap;
    private Date date;
    private String cellId;

    public Query1Outcome() {
    }

    public Query1Outcome(HashMap<String, HashSet<String>> typeMapInput){
        this.typeMap = new HashMap<>();
        for(String shipType: typeMapInput.keySet()){
            this.typeMap.put(shipType, typeMapInput.get(shipType).size());
        }
    }

    public Query1Outcome(HashMap<String, Integer> typeMap, Date date) {
        this.typeMap = typeMap;
        this.date = date;
    }

    public Map<String, Integer> getTypeMap() {
        return typeMap;
    }

    public void setTypeMap(HashMap<String, Integer> typeMap) {
        this.typeMap = typeMap;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getCellId() {
        return cellId;
    }

    public void setCellId(String cellId) {
        this.cellId = cellId;
    }
}
