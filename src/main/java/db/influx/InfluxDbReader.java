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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InfluxDbReader {

    static String tokenStr = "hcDUQzhkGzghAo3frtycAgthbpRZ0GUCSnOkrslnGvpS8kE7hfOSHsWtAPUZIirHukvvwMS-nvxepNIVM1vcqQ==";
    private static char[] token = tokenStr.toCharArray();
    private static String org = "blake";
    private static String bucket = "blake";

    public static void main(String[] args) {

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", token, org, bucket);

        //
        // Query data
        //

        String flux_str = "from(bucket: \"blake\") " +
                "    |> range(start: -2d,stop:now()) " +
                "    |> filter(fn: (r) => r._measurement == \"datapoint\" and r.sensor == \"sensor01\")" +
                "    |> mean()";

        String start="-2d";
        String end = "now()";
        String measurement = "datapoint";
        Map<String,String> tags = new HashMap<>();
        tags.put("sensor","sensor01");
        String aggregation = "count()";

         String flux = generateQuerySql(bucket,start,end,measurement,tags,aggregation);

         flux = String.format(flux_str);

        QueryApi queryApi = influxDBClient.getQueryApi();

        List<FluxTable> tables = queryApi.query(flux);
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
//                System.out.println(fluxRecord.getTime() + ": " + fluxRecord.getValueByKey("_value"));
                System.out.println(fluxRecord.getValueByKey("_start")+" ----> "+fluxRecord.getValueByKey("_stop") + ": " + fluxRecord.getValueByKey("_value"));
            }
        }

        influxDBClient.close();
    }


    public static String generateQuerySql(String bucket, String start, String end, String measurement, Map<String,String> tags
    ,String aggregation){

        StringBuilder tagSb = new StringBuilder() ;
        if(tags != null && tags.size()>0){
            for(Map.Entry<String,String> entry : tags.entrySet() ){
                tagSb.append(" and ");
                String tag = entry.getKey();
                String value = entry.getValue();
                tagSb.append("r.").append(tag).append("==").append(value);
            }
            tagSb.append(" )");

        }

        StringBuilder sb = new StringBuilder();
        sb.append("from (bucket: ").append(bucket).append(")").append(" |> range(start: ").append(start)
        .append(", end: ").append(end).append(")")
        .append(" |> filter(fn: (r) => r._measurement == ").append(measurement).append(tagSb.toString())
        .append(" |> ").append(aggregation);

        return sb.toString();
    }


}
