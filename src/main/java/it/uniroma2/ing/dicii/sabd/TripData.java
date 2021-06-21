package it.uniroma2.ing.dicii.sabd;

public class TripData {

    private static final double minLat = 32.0;
    private static final double maxLat = 45.0;
    private static final int stepsLat = 10;
    private static final double minLon = -6.0;
    private static final double maxLon = 37.0;
    private static final int stepsLon = 40;

    String tripId;
    String shipId;
    double lat;
    double lon;
    long timestamp;
    String cell;
    String shipType;

    public TripData(String tripId, String shipId, double lon, double lat, long timestamp, int shipType) {
        this.tripId = tripId;
        this.shipId = shipId;
        this.lon = lon;
        this.lat = lat;
        this.timestamp = timestamp;
        this.shipType = setShipType(shipType);
        this.cell = computeCell(lat, lon);
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
        char latId = 'A';
        double stepWidthLat = (maxLat-minLat)/stepsLat;
        int positionLat = (int)((lat-minLat)/stepWidthLat);
        latId += positionLat;

        int lonId = 1;
        double stepWidthLon = (maxLon-minLon)/stepsLon;
        int positionLon = (int)((lon-minLon)/stepWidthLon);
        lonId += positionLon;

        return "" + latId + lonId;
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
