package it.uniroma2.ing.dicii.sabd.flink.query3;

import it.uniroma2.ing.dicii.sabd.TripData;
import org.apache.flink.api.java.tuple.Tuple2;

public class Query3Accumulator {

    private Double distance;
    private Tuple2<Double, Double> lastPos;
    private static final double RADIUS = 6378.388;
    private long lastTimestamp;


    public Query3Accumulator() {
        this.distance = 0.0;
        this.lastPos = null;
    }

    public void add(TripData data) {

        Tuple2<Double,Double> pos = new Tuple2<>(data.getLat(), data.getLon());
        if (this.lastPos == null) {
            this.lastPos = pos;
        }

        this.distance = this.distance + this.computeDistance(this.lastPos, pos);

        this.lastPos = pos;
        this.lastTimestamp = data.getTimestamp();
    }

    private Double computeDistance(Tuple2<Double, Double> start, Tuple2<Double, Double> end) {
        return Math.sqrt(Math.pow(RADIUS *Math.PI/180*(end.f0-start.f0),2)+Math.pow(RADIUS *Math.PI/180*(end.f1-start.f1),2));
    }

    public Double getDistance() {
        return distance;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }
}
