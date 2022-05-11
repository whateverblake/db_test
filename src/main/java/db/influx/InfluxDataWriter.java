package db.influx;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class InfluxDataWriter {

    static String tokenStr = "hcDUQzhkGzghAo3frtycAgthbpRZ0GUCSnOkrslnGvpS8kE7hfOSHsWtAPUZIirHukvvwMS-nvxepNIVM1vcqQ==";
    private static char[] token = tokenStr.toCharArray();
    private static String org = "blake";
    private static String bucket = "blake";
    private static Random random = new Random();

    private static int size = 50;
    public static void main(String[] args) {

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", token, org, bucket);

        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
        int batch = 1 ;
        long start = System.currentTimeMillis();
        writeBatch(batch,size,writeApi);
        long end = System.currentTimeMillis();
        System.out.println("time consumer: "+(end-start));
        influxDBClient.close();

    }



    public static void writeBatch(int batch,int size,WriteApiBlocking writeApi){

        int iteration = (int)Math.ceil(size/batch);

        for (int i =0; i< iteration; i++){
            List<DataPoint> points = generatePoints(batch);
            writeApi.writeMeasurements(WritePrecision.NS,points);
        }

    }




    private static List<DataPoint> generatePoints(int batch){
        List<DataPoint> pointList = new ArrayList<>();
        for (int i =0 ; i < batch;i++){
            DataPoint dataPoint = new DataPoint();
            dataPoint.sensor = "sensor01";
            dataPoint.value = generateValue();
            dataPoint.time = Instant.now();
            pointList.add(dataPoint);
        }
        return pointList ;
    }


    private static Double generateValue(){
        return random.nextDouble()* 100 ;
    }









    @Measurement(name = "datapoint")
    private static class DataPoint {

        @Column(tag = true)
        String sensor;

        @Column
        Double value;


        @Column(timestamp = true)
        Instant time;
    }


}
