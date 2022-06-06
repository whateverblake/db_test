package db.influx;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.influxdb.client.*;
import com.influxdb.client.domain.DeletePredicateRequest;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import db.Main;
import db.entity.DataPoint;
import db.util.GenerateDataPointBatch;
import db.writer.BaseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

public class InfluxDataWriter extends BaseWriter {

    static String tokenStr = "hcDUQzhkGzghAo3frtycAgthbpRZ0GUCSnOkrslnGvpS8kE7hfOSHsWtAPUZIirHukvvwMS-nvxepNIVM1vcqQ==";
    private static char[] token = tokenStr.toCharArray();
    private static String org = "blake";
    private static String bucket = "blake";
    private static Random random = new Random();
    private static String measurement = "sensor_data";
    private static String url;
    private InfluxDBClient influxDBClient;
    private WriteApiBlocking writeApi;
    private DeleteApi deleteApi;
    private static Logger logger = LoggerFactory.getLogger(InfluxDataWriter.class);


    static {
        try {
            String tokenStr = System.getProperty("influx_token");
            token = tokenStr.toCharArray();
            org = System.getProperty("influx_org");
            bucket = System.getProperty("influx_bucket");
            url = System.getProperty("influx_url");
            measurement = System.getProperty("influx_measurement");
            logger.info("influxdb token = {}",tokenStr);
            logger.info("influxdb org = {}",org);
            logger.info("influxdb bucket = {}",bucket);
            logger.info("influxdb url = {}",url);
            logger.info("influxdb measurement = {}",measurement);
            if(org==null || bucket ==null || url ==null || measurement==null){
                throw new RuntimeException("influxdb parameters are not set");
            }
        }catch (Exception e){
            logger.info(".....please make sure all influxdb args are set........");
            System.exit(0);
        }

    }

    public InfluxDataWriter() {
        createClient();
        writeApi = influxDBClient.getWriteApiBlocking();
//        deleteApi = influxDBClient.getDeleteApi();
    }


    public void createClient() {
        influxDBClient = InfluxDBClientFactory.create(url, token, org, bucket);
    }


    @Override
    public void saveDataPoint(List<db.entity.DataPoint> dataPointList) {
        List<Point> points = dataPointToPoint(dataPointList);
        try{
            writeApi.writePoints(points);
        }catch (Exception e){
            logger.error("write influx data error ",e);
        }

    }


    public List<Point> dataPointToPoint(List<db.entity.DataPoint> dataPointList) {
        List<Point> points = new ArrayList<>();
        for (DataPoint dataPoint : dataPointList) {

            Point point = new Point(measurement);

           /* point.addField("value", dataPoint.getValue());
            point.addField("value1", dataPoint.getValue());
            point.addField("value2", dataPoint.getValue());
            point.addField("value3", dataPoint.getValue());
            point.addField("value4", dataPoint.getValue());*/

            generateField(point);

            Map<String, String> tags = new HashMap<>();
            for (String tag : dataPoint.getTags()) {
                tags.put(tag, tag);
            }
            point.addTags(tags);
            point.time(dataPoint.getTime(),WritePrecision.MS);
            points.add(point);
        }

        return points;


    }

    private void generateField(Point point){
        for(int i =0; i < 250; i++){
            StringBuilder fieldName = new StringBuilder("value").append(i);
            point.addField(fieldName.toString(), GenerateDataPointBatch.generateValue());
        }

    }


//    private static int size = 50;
//
//    public static void main(String[] args) {
//
//        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", token, org, bucket);
//
//        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
//        int batch = 1;
//        long start = System.currentTimeMillis();
//        writeBatch(batch, size, writeApi);
//        long end = System.currentTimeMillis();
//        System.out.println("time consumer: " + (end - start));
//        influxDBClient.close();
//
//    }

//
//    public static void writeBatch(int batch, int size, WriteApiBlocking writeApi) {
//
//        int iteration = (int) Math.ceil(size / batch);
//
//        for (int i = 0; i < iteration; i++) {
//            long subStart = System.currentTimeMillis();
//            List<DataPoint> points = generatePoints(batch);
//            writeApi.writeMeasurements(WritePrecision.NS, points);
//            long subEnd = System.currentTimeMillis();
//
//            System.out.println("write batch iteration :" + i + " consumer:" + (subEnd - subStart));
//        }
//
//    }


//    private static List<DataPoint> generatePoints(int batch) {
//        List<DataPoint> pointList = new ArrayList<>();
//        for (int i = 0; i < batch; i++) {
//            DataPoint dataPoint = new DataPoint();
//            dataPoint.sensor = "sensor01";
//            dataPoint.value = generateValue();
//            dataPoint.time = Instant.now();
//            pointList.add(dataPoint);
//        }
//        return pointList;
//    }
//
//
//    private static Double generateValue() {
//        return random.nextDouble() * 100;
//    }


//    @Measurement(name = "datapoint")
//    private static class DataPoint {
//
//        @Column(tag = true)
//        String sensor;
//
//        @Column
//        Double value;
//
//
//        @Column(timestamp = true)
//        Instant time;
//    }


}
