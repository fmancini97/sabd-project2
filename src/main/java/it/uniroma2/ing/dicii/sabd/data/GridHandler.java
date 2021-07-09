package it.uniroma2.ing.dicii.sabd.data;

public class GridHandler {

    public enum Sea{
        Occidentale,
        Orientale;
    }
    public static int lonIndexSeaSeparator;
    public final static double channelOfSicilyLon = 11.797697;
    private static final double minLat = 32.0;
    private static final double maxLat = 45.0;
    private static final int stepsLat = 10;
    private static final double minLon = -6.0;
    private static final double maxLon = 37.0;
    private static final int stepsLon = 40;
    private static final double stepWidthLat = (maxLat-minLat)/stepsLat;
    private static final double stepWidthLon = (maxLon-minLon)/stepsLon;

    private static void lonIndexSeaSeparatorInit(){
        double stepWidthLon = (maxLon-minLon)/stepsLon;
        double currentLon = minLon;
        double nextLon = currentLon + stepWidthLon;
        while(nextLon < channelOfSicilyLon) {
            currentLon = nextLon;
            nextLon = currentLon + stepWidthLon;
        }
        if(channelOfSicilyLon - currentLon < nextLon - channelOfSicilyLon){
            lonIndexSeaSeparator = (int) ((currentLon-minLon)/stepWidthLon);
        } else{
            lonIndexSeaSeparator = (int) ((nextLon-minLon)/stepWidthLon);
        }
    }

    public static String computeCell(double lat, double lon){
        char latId = 'A';
        int positionLat = (int)((lat-minLat)/stepWidthLat);
        latId += positionLat;

        int lonId = 1;
        int positionLon = (int)((lon-minLon)/stepWidthLon);
        lonId += positionLon;

        return "" + latId + lonId;
    }

    public static boolean computeValidity(double lat, double lon){
        return lat>=minLat && lat <=maxLat && lon>=minLon && lon<=maxLon;
    }

    static{
        lonIndexSeaSeparatorInit();
    }
}
