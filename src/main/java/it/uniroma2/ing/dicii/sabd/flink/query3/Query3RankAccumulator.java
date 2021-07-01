package it.uniroma2.ing.dicii.sabd.flink.query3;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

public class Query3RankAccumulator {

    private final static int SIZE = 5;
    private final TreeSet<Tuple2<String, Double>> ranking;
    private final Map<String, Tuple2<String, Double>> elements;

    public Query3RankAccumulator() {
        this.ranking = new TreeSet<>((o1, o2) -> {
            int result = o1.f1.compareTo(o2.f1);
            if (result == 0) {
                return o1.f0.compareTo(o2.f0);
            } else {
                return result;
            }
        });
        this.elements = new HashMap<>();
        for (int i = 1; i <= SIZE; i++) {
            this.ranking.add(new Tuple2<>("", (double) -i));
        }
    }

    public void add(Tuple2<String, Double> data) {
        if (this.elements.containsKey(data.f0)) {
            Tuple2<String, Double> elem = this.elements.get(data.f0);

            this.ranking.remove(elem);
            this.ranking.add(data);
            this.elements.put(data.f0, data);



        } else {
            Tuple2<String, Double> min = this.ranking.first();

            //System.out.println(min);
            //System.out.println(data);
            if (data.f1 > min.f1) {
                this.ranking.add(data);
                this.ranking.remove(min);
                this.elements.remove(min.f0);
                this.elements.put(data.f0, data);
            }

            if (this.ranking.size() !=5) {
                System.out.println("Errorerrrrrrrrrrrr");
            }


        }
    }

    public String getResult() {
        StringBuilder result = new StringBuilder();
        NavigableSet<Tuple2<String, Double>> rankResult = this.ranking.descendingSet();
        rankResult.forEach((element) -> result.append(",").append(element.f0).append(",").append(element.f1));
        return result.toString();
    }
}
