package it.uniroma2.ing.dicii.sabd.flink.query2;

import java.io.Serializable;
import java.util.*;

/**
 * For a single cell, it maintains a set of tripId that have travelled on that cell.
 */
public class Query2IntermediateAccumulator implements Serializable {

    public Set<String> getAttendances() {
        return attendances;
    }

    public void setAttendances(Set<String> attendances) {
        this.attendances = attendances;
    }

    Set<String> attendances;

    public Query2IntermediateAccumulator(){
        attendances = new HashSet<>();
    }

    public void add(String tripId) {
        attendances.add(tripId);
    }

}
