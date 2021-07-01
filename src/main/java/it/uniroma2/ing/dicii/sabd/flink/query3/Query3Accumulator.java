package it.uniroma2.ing.dicii.sabd.flink.query3;

import it.uniroma2.ing.dicii.sabd.TripData;

public class Query3Accumulator {

    private Double startLat, startLon;
    private Double currentLat, currentLon;

    public Query3Accumulator() {
    }

    public void add(TripData data) {
        this.currentLat = data.getLat();
        this.currentLon = data.getLon();

        this.startLat = (this.startLat != null) ? this.startLat : this.currentLat;
        this.startLon = (this.startLon != null) ? this.startLon : this.currentLon;

    }

    public double getDistance() {
        return Math.sqrt(Math.pow(currentLat - startLat, 2) + Math.pow(currentLon - startLon, 2));
    }

}
