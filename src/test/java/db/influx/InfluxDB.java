package db.influx;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.write.Point;

import java.util.*;

public class InfluxDB {


    private static int size = 50;
    static String tokenStr = "hcDUQzhkGzghAo3frtycAgthbpRZ0GUCSnOkrslnGvpS8kE7hfOSHsWtAPUZIirHukvvwMS-nvxepNIVM1vcqQ==";
    private static char[] token = tokenStr.toCharArray();
    private static String org = "blake";
    private static String bucket = "blake";
    private static Random random = new Random();
    private static String measurement = "hello_world1";

    public static void main(String[] args) {

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", token, org, bucket);

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
