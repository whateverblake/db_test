package db.influx;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class InfluxDB {


    private static int size = 50;
    static String tokenStr = "hcDUQzhkGzghAo3frtycAgthbpRZ0GUCSnOkrslnGvpS8kE7hfOSHsWtAPUZIirHukvvwMS-nvxepNIVM1vcqQ==";
    private static char[] token = tokenStr.toCharArray();
    private static String org = "blake";
    private static String bucket = "blake";
    private static Random random = new Random();
    private static String measurement = "hello_world1";
    private static Logger logger = LoggerFactory.getLogger(InfluxDB.class);

    public static void main(String[] args) {

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", token, org, bucket);
       doSearch(influxDBClient);


    }

    public static void doSearch(InfluxDBClient influxDBClient){
        String sql = "from (bucket: \"blake\") |> range(start: 1652716800, stop: 1652803200) |> filter(fn: (r) => r._measurement == \"hello6\"  and r.tag1== \"tag1\" and r.tag2== \"tag2\" ) |> count()";
//        String sql = "from(bucket: \"blake\") " +
//                "    |> range(start: 1652716800,stop:1652803200) " +
//                "    |> filter(fn: (r) => r._measurement == \"datapoint\" and r.sensor == \"sensor01\")" +
//                "    |> mean()";

        QueryApi queryApi = influxDBClient.getQueryApi();

        sql = String.format(sql);
        logger.info("*********** influx sql = {}",sql);
        List<FluxTable> tables = queryApi.query(sql);
        queryApi.query(sql);
    }


    public static void doWrite(InfluxDBClient influxDBClient){
        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
        int batch = 1;
        long start = System.currentTimeMillis();
        List<Point> pointList = new ArrayList<>();
        Point point = new Point(measurement);
        point.addField("value",1);
        Point point2 = new Point(measurement);
        point2.addField("value",3);
        Map<String,String> tags = new HashMap<>();
        tags.put("tag1","tag1");
        tags.put("tag2","tag2");
        point.addTags(tags);
        point2.addTags(tags);
        pointList.add(point);
        pointList.add(point2);
        writeApi.writePoints(pointList);
        long end = System.currentTimeMillis();
        System.out.println("time consumer: " + (end - start));
        influxDBClient.close();
    }

}
