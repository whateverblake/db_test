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

    public static void main(String[] args) {

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", token, org, bucket);

        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();

        writeApi.writePoints(generatePoints(1));

        influxDBClient.close();

    }





    private static List<Point> generatePoints(int batch){
        List<Point> pointList = new ArrayList<>();
        for (int i =0 ; i < batch;i++){
            Point point = Point.measurement("temperature")
                    .addTag("tag1", "tag1")
                    .addField("value", generateValue())
                    .time(Instant.now().toEpochMilli(), WritePrecision.NS);
            pointList.add(point);
        }

        return pointList ;
    }


    private static Double generateValue(){
        return random.nextDouble()* 100 ;
    }









    @Measurement(name = "temperature")
    private static class Temperature {

        @Column(tag = true)
        String location;

        @Column
        Double value;

        @Column
        String ff;

        @Column(timestamp = true)
        Instant time;
    }


}
