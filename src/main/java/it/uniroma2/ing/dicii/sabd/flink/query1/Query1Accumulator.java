package it.uniroma2.ing.dicii.sabd.flink.query1;

import java.io.Serializable;
import java.util.*;

public class Query1Accumulator implements Serializable {

    //mappa (tiponave - set(tripid))
    private HashMap<String, HashSet<String>> typeMap;


    public Query1Accumulator(){
        this.typeMap = new HashMap<>();
    }

    public Query1Accumulator(HashMap<String, HashSet<String>> typeMap) {
        this.typeMap = typeMap;
    }

    public void add(String shipType, HashSet<String> tripsSet){
        for (String tripId : tripsSet) {
            add(shipType, tripId);
        }
    }

    public void add(String shipType, String tripId){
        HashSet<String> typeSet = typeMap.get(shipType);
        //cell found but shipType not found in that cell
        if(typeSet == null){
            typeSet = new HashSet<>();
        }  //update value

        typeSet.add(tripId);
        typeMap.put(shipType, typeSet);
    }


    public HashMap<String, HashSet<String>> getTypeMap() {
        return typeMap;
    }

    public void setTypeMap(HashMap<String, HashSet<String>> typeMap) {
        this.typeMap = typeMap;
    }

}
