package db.kariosdb;


import db.entity.ReaderEntity;
import db.reader.BaseReader;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.*;
import org.kairosdb.client.builder.aggregator.SamplingAggregator;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.QueryResult;
import org.kairosdb.client.response.Result;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class KariosDBReader extends BaseReader {

    private static HttpClient httpClient;
    private static String url;

    static {
        url = System.getProperty("kairos_url");
    }

    public KariosDBReader() {
        createClient();
    }

    public void createClient() {
        try {
            httpClient = new HttpClient(url);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

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
//            query(httpClient, start, end, TimeUnit.MONTHS, metric, tags);
            long endTime = System.currentTimeMillis();
            System.out.println("time consume: " + (endTime - startTime));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Override
    public double doQuery(ReaderEntity readerEntity) {
        return  query(readerEntity.getStartTime(),readerEntity.getEndTime(),"sensor1",
                readerEntity.getTags(),readerEntity.getAggregation());
    }

    public static double query(long start, long end, String metric, Map<String, String> tags, String aggregation) {
        Double res = 0d ;
        QueryBuilder builder = QueryBuilder.getInstance();

        Date startDate = new Date(start);
        Date endDate = new Date(end);
        builder.setStart(startDate)
                .setEnd(endDate)
                .addMetric(metric)
                .addTags(tags)
                .addAggregator(createAggregator(aggregation));

        QueryResponse response = httpClient.query(builder);
        List<QueryResult> resultList = response.getQueries();
        if(resultList != null && resultList.size() >0){
            QueryResult queryResult = resultList.get(0);
//            List<Result> results = queryResult.getResults();
            res = Double.parseDouble(queryResult.getResults().get(0).getDataPoints().get(0).getValue().toString());
        }
        return res;
    }


    public static SamplingAggregator createAggregator(String aggregation) {
        SamplingAggregator samplingAggregator = null;
        switch (aggregation) {
            case "avg":
                samplingAggregator = AggregatorFactory.createAverageAggregator(
                        1, TimeUnit.YEARS
                );
                break;
            case "count":
                samplingAggregator = AggregatorFactory.createCountAggregator(
                        1, TimeUnit.YEARS
                );
                break;
            case "sum":
                samplingAggregator = AggregatorFactory.createSumAggregator(
                        1, TimeUnit.YEARS
                );
                break;

            case "max":
                samplingAggregator = AggregatorFactory.createMaxAggregator(
                        1, TimeUnit.YEARS
                );
                break;
            case "min":
                samplingAggregator = AggregatorFactory.createMinAggregator(
                        1, TimeUnit.YEARS
                );
                break;
        }

        return samplingAggregator ;

    }


}
