package it.uniroma2.ing.dicii.sabd.utils;


import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MetricSink implements SinkFunction<String> {

    // tuples counter
    private static long counter = 0L;
    // first output time
    private static long startTime = 0L;

    /**
     * Called when a new observation is seen, updates statistics
     */
    public static synchronized void incrementCounter() {
        if (startTime == 0L) {
            startTime = System.currentTimeMillis();
            System.out.println("Initialized!");
            return;
        }
        counter++;
        double currentTime = System.currentTimeMillis() - startTime;

        currentTime = currentTime/1000;

        // prints mean throughput and latency so far evaluated
        System.out.println("Mean throughput: " + (counter/currentTime) + "\n" + "Mean latency: " +
                (currentTime/counter));
    }

    @Override
    public void invoke(String value, Context context) {
        // stats counter
        incrementCounter();
    }
}
