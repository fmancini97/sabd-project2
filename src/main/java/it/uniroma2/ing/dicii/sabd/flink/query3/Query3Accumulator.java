package it.uniroma2.ing.dicii.sabd.flink.query3;

import it.uniroma2.ing.dicii.sabd.data.TripData;
import org.apache.flink.api.java.tuple.Tuple2;

public class Query3Accumulator {

    private Double distance;
    private Tuple2<Double, Double> lastPos; //lat lon
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

        this.distance = this.computeDistance(this.lastPos, pos);

        this.lastTimestamp = data.getTimestamp();
    }

    //Compute distance between P and Q
    private Double computeDistance(Tuple2<Double, Double> start, Tuple2<Double, Double> end) {
        double thetaP = toRadians(start.f0);
        double phiP = toRadians(start.f1);
        double thetaQ = toRadians(end.f0);
        double phiQ = toRadians(end.f1);
        double dLat = phiQ-phiP;

        return RADIUS*Math.acos(Math.cos(dLat)*Math.cos(thetaP)*Math.cos(thetaQ)+Math.sin(thetaP)*Math.sin(thetaQ));
    }

    private double toRadians(double value){
        return value*Math.PI/180;
    }

    public Double getDistance() {
        return distance;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }
}
