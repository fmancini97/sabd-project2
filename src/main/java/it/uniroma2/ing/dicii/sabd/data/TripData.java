package it.uniroma2.ing.dicii.sabd.data;

import static it.uniroma2.ing.dicii.sabd.data.GridHandler.computeValidity;

import java.io.Serializable;

public class TripData implements Serializable {


    private String tripId;
    private String shipId;
    private double lat;
    private double lon;
    private long timestamp;
    private String cell;
    private String shipType;
    private long dateAsTimestamp;
    boolean isValid;

    public long getDateAsTimestamp() {
        return dateAsTimestamp;
    }

    public void setDateAsTimestamp(long dateAsTimestamp) {
        this.dateAsTimestamp = dateAsTimestamp;
    }

    public TripData(String tripId, String shipId, double lon, double lat, long timestamp, int shipType, long dateAsTimestamp) {
        this.tripId = tripId;
        this.shipId = shipId;
        this.lon = lon;
        this.lat = lat;
        this.timestamp = timestamp;
        this.shipType = setShipType(shipType);
        this.cell = computeCell(lat, lon);
        this.dateAsTimestamp = dateAsTimestamp;
        this.isValid = computeValidity(lat,lon);
    }

    public boolean isValid(){
        return isValid;
    }

    private String setShipType(int shipType){
        if(shipType == 35){
            return String.valueOf(shipType);
        } else if (shipType >= 60 && shipType <= 69){
            return "60-69";
        } else if (shipType >= 70 && shipType <= 79){
            return "70-79";
        } else {
            return "others";
        }
    }

    private String computeCell(double lat, double lon){
        return GridHandler.computeCell(lat,lon);
    }

    public String getCell(){
        return cell;
    }

    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    public String getShipId() {
        return shipId;
    }

    public void setShipId(String shipId) {
        this.shipId = shipId;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getShipType(){
        return shipType;
    }

    public void setCell(String cell) {
        this.cell = cell;
    }

    public void setShipType(String shipType) {
        this.shipType = shipType;
    }
}
