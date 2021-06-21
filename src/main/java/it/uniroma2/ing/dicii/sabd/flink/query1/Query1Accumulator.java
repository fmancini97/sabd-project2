package it.uniroma2.ing.dicii.sabd.flink.query1;

import java.util.*;

public class Query1Accumulator {

    //mappa (cella - (tiponave - set(tripid)))
    private HashMap<String, Map<String, Set<String>>> cellsMap;

    public HashMap<String, Map<String, Set<String>>> getCellsMap() {
        return cellsMap;
    }

    public void setCellsMap(HashMap<String, Map<String, Set<String>>> cellsMap) {
        this.cellsMap = cellsMap;
    }

    public Query1Accumulator(){
        this.cellsMap = new HashMap<>();
    }

    public Query1Accumulator(HashMap<String, Map<String, Set<String>>> cellsMap){
        this.cellsMap = cellsMap;
    }

    public void add(String cell, String shipType, Set<String> tripsSet){
        for (String tripId : tripsSet) {
            add(cell, shipType, tripId);
        }
    }

    public void add(String cell, String shipType, String tripId){
        Map<String, Set<String>> setsForCell = cellsMap.get(cell);

        //cell not found
        if(setsForCell == null){
            HashMap<String, Set<String>> shiptypeCounters = new HashMap<>();
            Set<String> tripSet = new HashSet<>();
            tripSet.add(tripId);
            shiptypeCounters.put(shipType, tripSet);
            this.cellsMap.put(cell, shiptypeCounters);
        } else {
            Set<String> tripSet = setsForCell.get(shipType);
            //cell found but shipType not found in that cell
            if(tripSet == null){
                tripSet = new HashSet<>();
                tripSet.add(tripId);
                setsForCell.put(shipType, tripSet);
            } else {
                //update value
                tripSet.add(tripId);
                setsForCell.put(shipType, tripSet);
                this.cellsMap.put(cell, setsForCell);
            }
        }
    }



}
