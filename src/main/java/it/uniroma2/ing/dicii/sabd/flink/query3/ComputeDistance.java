package it.uniroma2.ing.dicii.sabd.flink.query3;

import it.uniroma2.ing.dicii.sabd.TripData;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class ComputeDistance extends RichMapFunction<TripData, Tuple2<String, Double>> {

    private transient ValueState<Tuple2<Double, Tuple2<Double, Double>>> travel;
    private static final double RADIUS = 6378.388;


    @Override
    public Tuple2<String, Double> map(TripData tripData) throws Exception {

        Tuple2<Double, Tuple2<Double, Double>> travelData = this.travel.value();

        if (travelData == null) {
            travelData = new Tuple2<>(0.0, new Tuple2<>(tripData.getLat(), tripData.getLon()));
        }

        Tuple2<Double, Double> position = new Tuple2<>(tripData.getLat(), tripData.getLon());

        double distance = travelData.f0 + this.computeDistance(travelData.f1, position);

        this.travel.update(new Tuple2<>(distance, position));

        return new Tuple2<>(tripData.getTripId(), distance);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<Tuple2<Double, Tuple2<Double, Double>>> descriptor = new ValueStateDescriptor<>( "travel", // the state name
                TypeInformation.of(new TypeHint<Tuple2<Double, Tuple2<Double, Double>>>() {}));


        this.travel = getRuntimeContext().getState(descriptor);
    }

    private Double computeDistance(Tuple2<Double, Double> start, Tuple2<Double, Double> end) {
        return Math.sqrt(Math.pow(RADIUS *Math.PI/180*(end.f0-start.f0),2)+Math.pow(RADIUS *Math.PI/180*(end.f1-start.f1),2));
    }

}
