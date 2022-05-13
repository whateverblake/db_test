package db.kariosdb;


import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.Metric;
import org.kairosdb.client.builder.MetricBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class KariosDBWriter {
    private static Random random = new Random();

    public static void main(String[] args) {

        try {
            HttpClient httpClient = new HttpClient("http://localhost:8080");
            int size = 50;
            int batch = 1;
            String metric = "sensor01";
            int iteration = (int) Math.ceil(size / batch);
            Map<String, String> tags = new HashMap<>();
            tags.put("tag1", "tag1");
            tags.put("tag2", "tag2");
            long startTime = System.currentTimeMillis();
            AtomicLong now = new AtomicLong(startTime);
            for (int i = 0; i < iteration; i++) {
                generateData(httpClient, batch, metric, tags,now);
            }
            long endTime = System.currentTimeMillis();
            System.out.println("time consume: " + (endTime - startTime));


        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static void generateData(HttpClient httpClient, int batch, String metric, Map<String, String> tags,AtomicLong now) {

        MetricBuilder builder = MetricBuilder.getInstance();

        Metric mt = builder.addMetric(metric);
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            String tag = entry.getKey();
            String value = entry.getValue();
            mt.addTag(tag, value);
        }

        for (int i = 0; i < batch; i++) {
            mt.addDataPoint(now.longValue(), generateValue());
            now.incrementAndGet();
        }

        httpClient.pushMetrics(builder);

    }


    private static Double generateValue() {
        return random.nextDouble() * 100;
    }


}
