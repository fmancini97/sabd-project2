package it.uniroma2.ing.dicii.sabd.flink.query3;



import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

public class Query3RankAccumulator {

    private final List<Tuple2<String, Double>> ranking;


    public Query3RankAccumulator() {
        this.ranking = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            this.ranking.add(new Tuple2<>(null, -1.0));
        }
    }

    public void add(Tuple2<String, Double> data) {
        Double distance = data.f1;
        int position = -1;
        while (position < 4 && distance > this.ranking.get(position+1).f1) {
            position++;
        }
        if (position != -1) {
            this.ranking.add(position, data);
            this.ranking.remove(0);
        }
    }

    public String getResult() {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            Tuple2<String, Double> position = this.ranking.get(4 - i);
            result.append(",").append(position.f0).append(",").append(String.format(Locale.ENGLISH, "%.2g",position.f1));
        }

        return result.toString();
    }
}
