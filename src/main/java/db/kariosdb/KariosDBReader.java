package db.kariosdb;


import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.*;
import org.kairosdb.client.response.QueryResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class KariosDBReader {
    private static Random random = new Random();

    public static void main(String[] args) {

        try {
            HttpClient httpClient = new HttpClient("http://localhost:8080");

            int start = 2;
            int end = 0;
            String metric = "sensor1";
            Map<String, String> tags = new HashMap<>();
            tags.put("tag1", "tag1");
            tags.put("tag2", "tag2");
            long startTime = System.currentTimeMillis();
            query(httpClient, start, end, TimeUnit.MONTHS, metric, tags);
            long endTime = System.currentTimeMillis();
            System.out.println("time consume: " + (endTime - startTime));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static void query(HttpClient httpClient, int start, int end, TimeUnit timeUnit, String metric, Map<String, String> tags) {
        QueryBuilder builder = QueryBuilder.getInstance();

        builder.setStart(start, timeUnit)
                .setEnd(end, timeUnit)
                .addMetric(metric)
                .addTags(tags)
                .addAggregator(AggregatorFactory
                        .createAverageAggregator(5, TimeUnit.MINUTES));

        QueryResponse response = httpClient.query(builder);
        System.out.println(response);


    }


}
