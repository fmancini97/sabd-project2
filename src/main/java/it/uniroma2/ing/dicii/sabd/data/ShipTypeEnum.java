package it.uniroma2.ing.dicii.sabd.data;

public enum ShipTypeEnum {

    MILITARI("35"),
    PASSEGGERI("60-69"),
    CARGO("70-79"),
    OTHERS("others");

    private final String shipType;

    private ShipTypeEnum(String shipType){
        this.shipType = shipType;
    }

    public String getShipType(){
        return this.shipType;
    }
}
