package it.uniroma2.ing.dicii.sabd;

import it.uniroma2.ing.dicii.sabd.utils.JSONTools;
import it.uniroma2.ing.dicii.sabd.utils.KafkaProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Injector {
    private static final SimpleDateFormat[] dateFormats = {new SimpleDateFormat("dd/MM/yy HH:mm"),
            new SimpleDateFormat("dd-MM-yy HH:mm")};
    private static final String defaultFlinkURL = "localhost:8081";
    private static final String datasetPath = "/data/dataset.csv";
    private static final String jobsAPI = "v1/jobs";
    private static final String jobName = "SABD Project 2";


    public static void main( String[] args ) {

        Logger log = Logger.getLogger(Injector.class.getSimpleName());
        //log.setLevel(Level.INFO);

        log.info("Parsing parameter");

        if (args.length < 2) {
            log.log(Level.WARNING, "Usage: {0} <minutes> <waitFlinkJob[true/FALSE]> [<flinkURL>]", Injector.class.getName());
            System.exit(-1);
        }

        long timeRange = 0;

        try {
            timeRange = Long.parseLong(args[0]);
        } catch (NumberFormatException e) {
            log.log(Level.WARNING, "Wrong number format: {0}", args[0]);
            System.exit(-1);
        }

        boolean waitFlinkJob = args[1].equalsIgnoreCase("true");

        String flinkURL = defaultFlinkURL;
        if (waitFlinkJob && args.length == 3) flinkURL = args[2];


        log.log(Level.INFO, "Starting Injector with timeRange = {0} minutes", timeRange);

        // Converting timeRange from minutes to milliseconds
        timeRange = timeRange * 60 * 1000;

        log.info("Reading data from file");
        Map<Long, List<String>> map = new TreeMap<>();
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;



        try {
            BufferedReader reader = new BufferedReader(new FileReader(Injector.datasetPath));
            String line;
            reader.readLine(); //read header
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                String dateString = values[7];

                Long timestamp = null;
                for (SimpleDateFormat dateFormat: dateFormats) {
                    try {
                        timestamp = dateFormat.parse(dateString).getTime();
                        break;
                    } catch (ParseException ignored) { }
                }

                if (timestamp == null) {
                    log.log(Level.SEVERE, "Unable to parse the date: {0}", dateString);
                    System.exit(-1);
                }

                min = (min < timestamp) ? min : timestamp;
                max = (max > timestamp) ? max : timestamp;
                List<String> records = map.computeIfAbsent(timestamp, k -> new ArrayList<>());
                records.add(line);
            }
            reader.close();
        } catch (IOException e) {
            log.log(Level.SEVERE, "Error while reading file: {0}", e.getMessage());
        }

        boolean jobReady = false;
        while (waitFlinkJob && !jobReady) {
            try {
                log.info("Flink Job is not ready");
                TimeUnit.SECONDS.sleep(5);
                jobReady = checkJobAvailability(flinkURL);
            } catch (IOException e) {
                log.log(Level.WARNING, "Error while checking flink status: {}", e.getMessage());
                System.exit(-1);
            } catch (InterruptedException e) {
                log.log(Level.WARNING, "Error while Injector sleeping: {}", e.getMessage());
            }
        }

        Properties props = KafkaProperties.getInjectorProperties();
        Producer<Long, String> producer = new KafkaProducer<>(props);

        Set<Map.Entry<Long, List<String>>> timeSet = map.entrySet();

        double proportion = timeRange / (double) (max - min);
        Long previous = null;

        log.info("Starting sending records");
        for (Map.Entry<Long, List<String>> entry: timeSet) {
            Long timestamp = entry.getKey();

            if (previous != null) {
                try {
                    long sleepTime =  (long) ((timestamp - previous) * proportion);
                    TimeUnit.MILLISECONDS.sleep(sleepTime);
                } catch (InterruptedException e) {
                    log.log(Level.WARNING, "Error while thread was sleeping: {0}", e.getMessage());
                }
            }

            for (String record : entry.getValue()) {

                ProducerRecord<Long, String> producerRecord = new ProducerRecord<>(KafkaProperties.SOURCE_TOPIC, 0, timestamp, timestamp, record);

                log.log(Level.FINER, "Record: {0}", record);
                producer.send(producerRecord, (recordMetadata, e) -> {e.printStackTrace();});
            }

            previous = timestamp;
        }
        producer.flush();
        producer.close();

        log.info("Finished sending records");

    }


    private static boolean checkJobAvailability(String flinkURL) throws IOException {
        String jobsAPIURL = String.format("http://%s/%s", flinkURL, jobsAPI);
        JSONObject jobsState = JSONTools.readJsonFromUrl(jobsAPIURL);

        JSONArray jobs = jobsState.getJSONArray("jobs");

        for (Object job: jobs) {
            JSONObject jobJSON = (JSONObject) job;
            String jobID = jobJSON.getString("id");
            JSONObject jobState = JSONTools.readJsonFromUrl(String.format("%s/%s", jobsAPIURL,jobID));
            if (jobState.getString("name").equals(jobName) && jobState.getString("state").equals("RUNNING"))
                return true;
        }
        return false;
    }
}
