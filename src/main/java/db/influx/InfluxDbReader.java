package db.influx;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.influxdb.client.*;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import db.entity.ReaderEntity;
import db.reader.BaseReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class InfluxDbReader extends BaseReader {

    static String tokenStr = "hcDUQzhkGzghAo3frtycAgthbpRZ0GUCSnOkrslnGvpS8kE7hfOSHsWtAPUZIirHukvvwMS-nvxepNIVM1vcqQ==";
    private static char[] token = tokenStr.toCharArray();
    private static String org = "blake";
    private static String bucket = "blake";
    private static String measurement = "sensor_data";
    private static String query_field = "value";

    private static String url;
    private InfluxDBClient influxDBClient;
    private QueryApi queryApi;
    private static Logger logger = LoggerFactory.getLogger(InfluxDbReader.class);


    static {
        try {
            String tokenStr = System.getProperty("influx_token");
            token = tokenStr.toCharArray();
            org = System.getProperty("influx_org");
            bucket = System.getProperty("influx_bucket");
            url = System.getProperty("influx_url");
            measurement = System.getProperty("influx_measurement");
            query_field = System.getProperty("influx_query_field","value");
            logger.info("influxdb token = {}",tokenStr);
            logger.info("influxdb org = {}",org);
            logger.info("influxdb bucket = {}",bucket);
            logger.info("influxdb url = {}",url);
            logger.info("influxdb measurement = {}",measurement);
            logger.info("influxdb query_field = {}",query_field);
            if (org == null || bucket == null || url == null || measurement == null) {
                throw new RuntimeException("influxdb parameters are not set");
            }
        } catch (Exception e) {
            logger.info(".....please make sure all influxdb args are set........");
            System.exit(0);
        }

    }

    public InfluxDbReader() {
        createClient();
        queryApi = influxDBClient.getQueryApi();

    }

    public void createClient() {
        influxDBClient = InfluxDBClientFactory.create(url, token, org, bucket);
    }


    @Override
    public double doQuery(ReaderEntity readerEntity) {
        Double value = 0d;
        String fluxSql = generateQuerySql(readerEntity.getStartTime(), readerEntity.getEndTime(), readerEntity.getTags(),
                readerEntity.getAggregation());
        String flux = String.format(fluxSql);
        logger.info("*********** influx sql = {}",flux);
        List<FluxTable> tables = queryApi.query(flux);
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                System.out.println(fluxRecord.getValueByKey("_start") + " ----> " + fluxRecord.getValueByKey("_stop") + ": " + fluxRecord.getValueByKey("_value"));
                logger.info(fluxRecord.getValueByKey("_start") + " ----> " + fluxRecord.getValueByKey("_stop") + ": " + fluxRecord.getValueByKey("_value"));
                value = Double.parseDouble(fluxRecord.getValueByKey("_value").toString());
            }
        }


        influxDBClient.close();

        return value;
    }

    /*public static void main(String[] args) {

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", token, org, bucket);

        //
        // Query data
        //

        String flux_str = "from(bucket: \"blake\") " +
                "    |> range(start: -2d,stop:now()) " +
                "    |> filter(fn: (r) => r._measurement == \"datapoint\" and r.sensor == \"sensor01\")" +
                "    |> mean()";

        String start = "-2d";
        String end = "now()";
        String measurement = "datapoint";
        Map<String, String> tags = new HashMap<>();
        tags.put("sensor", "sensor01");
        String aggregation = "count()";

        String flux = generateQuerySql(bucket, start, end, tags, aggregation);

        flux = String.format(flux_str);

        QueryApi queryApi = influxDBClient.getQueryApi();

        List<FluxTable> tables = queryApi.query(flux);
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
//                System.out.println(fluxRecord.getTime() + ": " + fluxRecord.getValueByKey("_value"));
                System.out.println(fluxRecord.getValueByKey("_start") + " ----> " + fluxRecord.getValueByKey("_stop") + ": " + fluxRecord.getValueByKey("_value"));
            }
        }

        influxDBClient.close();
    }*/


    public static String generateQuerySql(long start, long end, Map<String, String> tags
            , String aggregation) {

        start = start / 1000;
        end = end / 1000;
        StringBuilder tagSb = new StringBuilder();
        if (tags != null && tags.size() > 0) {
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                tagSb.append(" and ");
                String tag = entry.getKey();
                String value = entry.getValue();
                tagSb.append("r.").append(tag).append("== \"").append(value).append("\"");
            }
            tagSb.append(" )");

        }

        StringBuilder sb = new StringBuilder();
        sb.append("from (bucket: \"").append(bucket).append("\")").append(" |> range(start: ").append(start)
                .append(", stop: ").append(end).append(")")
                .append(" |> filter(fn: (r) => r._measurement == \"").append(measurement).append("\" ").append(tagSb.toString())
                .append(" |> filter(fn: (r) => r._field == \"").append(query_field).append("\" ").append(tagSb.toString());

        if(!aggregation.equals("list")){
            sb.append(" |> ").append(aggregation).append("()");
        }

        return sb.toString();
    }


}
